build:
	mkdir -p build
	g++ src/tictoc.cpp -o build/tictoc

clean:
	rm -r build
