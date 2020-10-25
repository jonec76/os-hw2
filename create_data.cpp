#include <fstream>
#include <iostream>
#include <random>

using namespace std;


// argv[1] = 11500000 for generating about 1G
int main(int argc, char* argv[]) {
    if (argc != 2)
        printf("Wrong parameters for create_data");

    long long int a = 1ULL << 31;
    a = a * -1;
    long long int b = 1ULL << 31;
    b = b - 1;
    random_device rd;
    default_random_engine gen = default_random_engine(rd());
    uniform_int_distribution<int> dis(-1000, 1000);
    fstream file;
    file.open("input.csv", ios::out);


    if (file.fail()) {
        cout << "Can't open file" << endl;
    } else {
        for (long long i = 0; i < atoi(argv[1]); ++i) {
            file << i;
            for(int j=0;j<19;j++)
                file<<"|" << dis(gen);
            file << endl;
            
        }
        cout << "Create complete" << endl;
        file.close();
    }
    return 0;
}
