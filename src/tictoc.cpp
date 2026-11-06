// TicToc optimistic concurrency control (Yu et al., SIGMOD 2016).
//
// Idea: instead of assigning a transaction a timestamp up front, each data
// item carries a validity interval [wts, rts]:
//   wts = timestamp of the commit that wrote the current value
//   rts = the latest timestamp at which that value is known to still be valid
// A transaction runs optimistically, remembering (wts, rts) of everything it
// reads and buffering its writes. At commit it *computes* a commit timestamp
//   commit_ts = max( wts of every item read,  rts+1 of every item written )
// and validates that every value it read is still valid at commit_ts, i.e.
// wts unchanged, and rts can be extended to >= commit_ts without conflicting
// with a concurrent writer. Writes are then installed with wts = rts = commit_ts.
//
// Concurrency: no global lock. Each item has a short latch (protects the
// {value, wts, rts} triple) and a commit-lock owner word. Write-set items are
// commit-locked in index order (so no deadlock); validation never waits on a
// commit lock, it aborts instead. Transaction state is touched only by the
// owning thread, so the registry mutex only guards map lookup/insert/erase.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <random>
#include <thread>
#include <cstdint>

using namespace std;

enum class TxStatus {
    ACTIVE,
    COMMITTED,
    ABORTED
};

using Timestamp = uint64_t;

// Snapshot of what a transaction observed when it read an item.
struct ReadEntry {
    int index;
    int value;
    Timestamp wts;
    Timestamp rts;
};

struct Transaction {
    int id = 0;
    TxStatus status = TxStatus::ACTIVE;

    vector<ReadEntry> readSet;
    vector<pair<int, int>> writeSet;
};

struct DataItem {
    mutex latch;                  // protects value, wts, rts
    atomic<int> owner{0};         // id of tx holding the commit lock, 0 = free
    int value = 0;
    Timestamp wts = 0;
    Timestamp rts = 0;
};

class TransactionManager {
private:
    atomic<int> nextId{1};

    // Registry only maps id -> per-transaction state. Held briefly.
    map<int, shared_ptr<Transaction>> txns;
    mutex registryMutex;

    vector<DataItem> dataItems;

    shared_ptr<Transaction> getTransaction(int id) {
        lock_guard<mutex> guard(registryMutex);
        auto it = txns.find(id);
        return it == txns.end() ? nullptr : it->second;
    }

    void finish(const shared_ptr<Transaction>& tx, TxStatus s) {
        tx->status = s;
        tx->readSet.clear();
        tx->writeSet.clear();
        lock_guard<mutex> guard(registryMutex);
        txns.erase(tx->id);
    }

    void unlockAll(const vector<int>& locked, int id) {
        for (int x : locked) dataItems[x].owner.store(0);
        (void)id;
    }

public:
    TransactionManager(int m) : dataItems(m) {}

    int begin_trans() {
        auto tx = make_shared<Transaction>();
        tx->id = nextId.fetch_add(1);
        lock_guard<mutex> guard(registryMutex);
        txns.emplace(tx->id, tx);
        return tx->id;
    }

    void read(int id, int x, int& l) {
        auto tx = getTransaction(id);
        if (!tx || tx->status != TxStatus::ACTIVE)
            return;

        if (x < 0 || x >= static_cast<int>(dataItems.size())) {
            finish(tx, TxStatus::ABORTED);
            return;
        }

        for (const auto& [index, value] : tx->writeSet) {
            if (index == x) { l = value; return; }
        }
        for (const auto& r : tx->readSet) {
            if (r.index == x) { l = r.value; return; }
        }

        // Atomic snapshot of (value, wts, rts) under the item latch.
        DataItem& item = dataItems[x];
        ReadEntry e;
        {
            lock_guard<mutex> g(item.latch);
            e = { x, item.value, item.wts, item.rts };
        }
        l = e.value;
        tx->readSet.push_back(e);
    }

    void write(int id, int x, int l) {
        auto tx = getTransaction(id);
        if (!tx || tx->status != TxStatus::ACTIVE)
            return;

        if (x < 0 || x >= static_cast<int>(dataItems.size())) {
            finish(tx, TxStatus::ABORTED);
            return;
        }

        for (auto& [index, value] : tx->writeSet) {
            if (index == x) { value = l; return; }
        }
        tx->writeSet.emplace_back(x, l);
    }

    char tryC(int id) {
        auto tx = getTransaction(id);
        if (!tx || tx->status != TxStatus::ACTIVE)
            return 'a';

        // Phase 1: commit-lock the write set in index order (deadlock-free).
        sort(tx->writeSet.begin(), tx->writeSet.end());
        vector<int> locked;
        for (const auto& [x, v] : tx->writeSet) {
            int expected = 0;
            while (!dataItems[x].owner.compare_exchange_weak(expected, id)) {
                expected = 0;
                this_thread::yield();
            }
            locked.push_back(x);
        }

        // Phase 2: compute commit timestamp. While we hold a write lock, no
        // other validator can extend that item's rts, so rts+1 is stable.
        Timestamp commitTs = 0;
        for (const auto& [x, v] : tx->writeSet) {
            lock_guard<mutex> g(dataItems[x].latch);
            commitTs = max(commitTs, dataItems[x].rts + 1);
        }
        for (const auto& r : tx->readSet)
            commitTs = max(commitTs, r.wts);

        // Phase 3: validate the read set at commitTs, extending rts if needed.
        for (const auto& r : tx->readSet) {
            DataItem& item = dataItems[r.index];
            lock_guard<mutex> g(item.latch);

            if (item.wts != r.wts) {          // overwritten since we read it
                unlockAll(locked, id);
                finish(tx, TxStatus::ABORTED);
                return 'a';
            }
            if (item.rts < commitTs) {
                int o = item.owner.load();
                if (o != 0 && o != id) {      // a concurrent writer may commit
                    unlockAll(locked, id);    // at a ts that breaks our read
                    finish(tx, TxStatus::ABORTED);
                    return 'a';
                }
                item.rts = commitTs;          // extend validity interval
            }
        }

        // Phase 4: install writes and release locks.
        for (const auto& [x, v] : tx->writeSet) {
            DataItem& item = dataItems[x];
            {
                lock_guard<mutex> g(item.latch);
                item.value = v;
                item.wts = commitTs;
                item.rts = commitTs;
            }
            item.owner.store(0);
        }

        finish(tx, TxStatus::COMMITTED);
        return 'c';
    }
};

int n, m, numTrans, constVal;
double lambda, readRatio;

// Shared transaction manager
TransactionManager* tmPtr = nullptr;

// Log file and mutex
ofstream logFile;
mutex logMutex;

// Clock
using Clock = chrono::steady_clock;

const auto programStart = Clock::now();

// --------------------------------------------------
// Thread statistics
// --------------------------------------------------

struct ThreadStats
{
    long long committed = 0;
    long long aborted = 0;
    double totalCommitDelayMs = 0.0;
};

// --------------------------------------------------
// Get system time in milliseconds
// --------------------------------------------------

double getSysTime()
{
    return chrono::duration<double, milli>(
        Clock::now() - programStart
    ).count();
}

// --------------------------------------------------
// Generate a random integer in [0, k]
// --------------------------------------------------

int getRand(int k, mt19937& rng)
{
    uniform_int_distribution<int> dist(0, k);
    return dist(rng);
}

// --------------------------------------------------
// Generate a random floating-point value in [0, 1)
// --------------------------------------------------

double getFloatRand(mt19937& rng)
{
    uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng);
}

// --------------------------------------------------
// Generate exponential random delay
// Mean delay = lambda milliseconds
// --------------------------------------------------

double getExpRand(double mean, mt19937& rng)
{
    if (mean == 0.0)
        return 0.0;

    exponential_distribution<double> dist(1.0 / mean);

    return dist(rng);
}

// --------------------------------------------------
// Thread-safe logging
// --------------------------------------------------

void logLine(const string& message)
{
    lock_guard<mutex> lock(logMutex);

    logFile << message << '\n';
}

// --------------------------------------------------
// Worker thread
// Each thread executes one logical transaction.
// An aborted transaction is retried until commit.
// --------------------------------------------------

void updMem(int threadId, ThreadStats& stats, int numTxns) {
    // Initialize random number generator
    mt19937 rng( random_device{}() ^ (static_cast<unsigned int>(threadId) + 1U));


    // Execute this thread's share of the transactions, one after another
    for (int txn = 0; txn < numTxns; txn++) {
        // Initialize abort count
        int abortCnt = 0;

        // Generate read probability once per thread
        double readChance = getFloatRand(rng);

        // Determine transaction type
        bool readOnly = (readChance < readRatio);

        // Record critical section start time
        auto critStartTime = Clock::now();

        // Retry until the transaction commits
        char status = 'a';
        do {
            // Begin a new transaction
            int id = tmPtr->begin_trans();

            logLine(
                "Thread " + to_string(threadId) +
                " Transaction " + to_string(id) +
                (readOnly ? " read-only" : " read-write") +
                " begins at time " +
                to_string(getSysTime()) + " ms"
            );

            // Generate random iteration count in [0, m]
            int randIters = getRand(m, rng);

            // Execute transaction operations
            int locVal;
            for (int i = 0; i < randIters; i++) {
                int randInd = getRand(m - 1, rng);
                int randVal = getRand(constVal, rng);

                // Read shared value into local variable
                tmPtr->read(id, randInd, locVal);

                logLine(
                    "Thread " + to_string(threadId) +
                    " Transaction " + to_string(id) +
                    " reads from " + to_string(randInd) +
                    " a value " + to_string(locVal) +
                    " at time " + to_string(getSysTime()) + " ms"
                );

                // Write only if the transaction is not read-only
                if (readOnly == false) {
                    // Update the local value
                    locVal += randVal;
                    tmPtr->write(id, randInd, locVal);

                    logLine(
                        "Thread " + to_string(threadId) +
                        " Transaction " + to_string(id) +
                        " writes to " + to_string(randInd) +
                        " a value " + to_string(locVal) +
                        " at time " + to_string(getSysTime()) + " ms"
                    );
                }

                // Generate exponential computation delay
                double randTime = getExpRand(lambda, rng);
                this_thread::sleep_for(
                    chrono::duration<double, milli>(randTime)
                );
            }

            // Attempt to commit the transaction
            status = tmPtr->tryC(id);

            logLine(
                "Thread " + to_string(threadId) +
                " Transaction " + to_string(id) +
                " tryCommits with result " +
                (status == 'c' ? "commit" : "abort") +
                " at time " + to_string(getSysTime()) + " ms"
            );

            // Increment abort count after every attempt
            if (status != 'c') abortCnt++;

        } while (status != 'c');

        // Record critical section end time
        auto critEndTime = Clock::now();

        // Calculate total commit delay
        double commitDelay =
            chrono::duration<double, milli>(
                critEndTime - critStartTime
            ).count();

        // Update transaction statistics
        stats.committed++;
        stats.aborted += abortCnt;
        stats.totalCommitDelayMs += commitDelay;

        logLine(
            "Thread " + to_string(threadId) +
            " commitDelay " + to_string(commitDelay) +
            " ms abortCnt " + to_string(abortCnt)
        );
    }
}

// --------------------------------------------------
// Main function
// --------------------------------------------------

int main()
{
    // Read input parameters
    ifstream input("inp-params.txt");

    if (!input || !(input >> n >> m >> numTrans >> constVal >> lambda >> readRatio)) {
        cerr << "Invalid inp-params.txt\n";
        return 1;
    }

    // Validate parameters
    if (n <= 0 || m <= 0 || numTrans <= 0 || constVal < 0 ||
        !isfinite(lambda) || lambda < 0.0 ||
        !isfinite(readRatio) ||
        readRatio < 0.0 || readRatio > 1.0) {
        cerr << "Invalid parameters\n";
        return 1;
    }

    input.close();

    // Create shared array of size m
    // TransactionManager manages the shared data.
    TransactionManager tm(m);
    tmPtr = &tm;

    // Open log file
    logFile.open("TicToc-log.txt");
    if (!logFile) {
        cerr << "Cannot create TicToc-log.txt\n";
        return 1;
    }

    // Create n worker threads
    vector<thread> threads;

    // Create statistics for each thread
    vector<ThreadStats> stats(n);

    // Create n updtMem threads
    for (int i = 0; i < n; i++) {
        threads.emplace_back(
            updMem,
            i + 1,
            ref(stats[i]),
            numTrans / n + (i < numTrans % n ? 1 : 0)
        );
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Close log file
    logFile.close();

    // Calculate total statistics
    long long totalCommitted = 0;
    long long totalAborted = 0;
    double totalDelayMs = 0.0;

    for (const auto& s : stats) {
        totalCommitted += s.committed;
        totalAborted += s.aborted;
        totalDelayMs += s.totalCommitDelayMs;
    }

    // Display execution statistics
    cout << fixed << setprecision(3);

    cout << "Committed transactions: " << totalCommitted << '\n';
    cout << "Aborted attempts: " << totalAborted << '\n';

    if (totalCommitted > 0) {
        cout << "Average commit delay: "
             << totalDelayMs / totalCommitted
             << " ms\n";

        cout << "Average aborts per committed transaction: "
             << static_cast<double>(totalAborted) / totalCommitted
             << '\n';
    }

    cout << "Log written to TicToc-log.txt\n";

    return 0;
}
