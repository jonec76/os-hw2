#include <stdio.h>
#include <stdlib.h>
#include <string.h> 
#include <fstream>
#include <sstream>
#include <string>
#include <iostream>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <mutex>

using namespace std;
#define NUM_THREADS 10
size_t limit = 2; 
mutex mu;

typedef struct {
    char** line;
    size_t limit;
} Param;

double array[NUM_THREADS];
Param param_obj[NUM_THREADS];

void str_to_json(char* line){
    size_t ln = strlen(line) - 1;
    if (*line && line[ln] == '\n') 
        line[ln] = '\0';

    int index = 1;
    char* token = strtok(line, "|"); 
    
    while (token != NULL) { 
        if(index != 20)
            printf("\"col_%d\":\"%s\",\n", index++, token); 
        else
            printf("\"col_%d\":\"%s\"\n", index++, token); 
        token = strtok(NULL, "|"); 
    } 
    assert(index == 21);
}

void *threaded_task(void *param) {
    Param* p = (Param*) param;
    mu.lock();

    for(size_t i=0;i<p->limit;i++){
        printf("%s\n", p->line[i]);
        // str_to_json(p->line[i++]);
    }
    mu.unlock();

    pthread_exit((void *)param);
}

int main() {
    char const* const input_file = "input.csv"; 
    char const* const output_file = "output.json"; 
    FILE* in_file = fopen(input_file, "r"); 
    FILE* out_file = fopen(output_file, "w"); 
    char line[1024];

    pthread_attr_t attr;
    pthread_t thread[NUM_THREADS];
    int rc;
    size_t t=0;
    void *status;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    
    size_t limit_ctr = 0;
    bool new_obj = true;
    size_t thread_in_use = 0;
    while (fgets(line, sizeof(line), in_file)) {
        if(new_obj){
            param_obj[t].line = new char* [limit];
            param_obj[t].limit = limit;
            new_obj = false;
        }
        param_obj[t].line[limit_ctr] = (char*)malloc(1024*(sizeof(char)));
        strcpy(param_obj[t].line[limit_ctr], line);
        
        if(limit_ctr == limit - 1){
            rc = pthread_create(&thread[t], &attr, threaded_task, (void *)&param_obj[t]);
            if (rc) {
                printf("ERROR: return code from pthread_create() is %d\n", rc);
                exit(-1);
            }
            thread_in_use++;
            limit_ctr = -1;
            new_obj = true;
            t++;
        }

        limit_ctr++;
    }
    pthread_attr_destroy(&attr);
    for (t = 0; t < thread_in_use; t++) {
        rc = pthread_join(thread[t], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
    }
    thread_in_use = 0;


    // fprintf (out_file, "]");
    fclose(in_file);
    fclose(out_file);
}