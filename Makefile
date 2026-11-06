build:
	mkdir -p build
	g++ src/mvto.cpp -o build/mvto
	g++ src/tictoc.cpp -o build/tictoc

clean:
	rm -r build
