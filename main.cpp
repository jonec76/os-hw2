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
mutex mu;

typedef struct {
    char* line;
} Param;

double array[NUM_THREADS];
Param p_obj[NUM_THREADS];

void str_to_json(char* line){
    mu.lock();
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
    mu.unlock();
}

void str_to_json(char* line, FILE** out_file){
    size_t ln = strlen(line) - 1;
    if (*line && line[ln] == '\n') 
        line[ln] = '\0';

    fprintf (*out_file, "{");
    int index = 1;
    char* token = strtok(line, "|"); 
    
    while (token != NULL) { 
        if(index != 20)
            fprintf(*out_file, "\"col_%d\":\"%s\",\n", index++, token); 
        else
            fprintf(*out_file, "\"col_%d\":\"%s\"\n", index++, token); 
        token = strtok(NULL, "|"); 
    } 
    assert(index == 21);
    fprintf (*out_file, "}");
}

void *threaded_task(void *param) {
    Param* p = (Param*) param;
    str_to_json(p->line);
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
    long t=0;
    void *status;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    
    // fprintf (out_file, "[");

    while (fgets(line, sizeof(line), in_file)) {
        printf("Creating thread %ld\n", t);
        p_obj[t].line = (char*)malloc(1024*(sizeof(char)));
        strcpy(p_obj[t].line, line);
        rc = pthread_create(&thread[t], &attr, threaded_task, (void *)&p_obj[t]);
        if (rc) {
            printf("ERROR: return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
        t++;
    }
    pthread_attr_destroy(&attr);
    for (t = 0; t < NUM_THREADS; t++) {
        rc = pthread_join(thread[t], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
    }


    // fprintf (out_file, "]");
    fclose(in_file);
    fclose(out_file);
}