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
#define line_size 1024
#define max_line_per_thread 40

using namespace std;

const size_t word_per_line = 20; 

mutex mu;

typedef struct {
    char** line;
    char* result;
    size_t line_per_thread;
} Param;

void str_to_json(char** result, char* line){
    size_t ln = strlen(line) - 1;
    if (*line && line[ln] == '\n') 
        line[ln] = '\0';

    size_t word_idx = 0;
    char* token = strtok(line, "|"); 
    
    while (token != NULL) { 
        char tmp[64];
        if(word_idx < word_per_line-1){
            sprintf(tmp, "\"col_%ld\":\"%s\",\n", word_idx++, token);
        } else{
            sprintf(tmp, "\"col_%ld\":\"%s\"\n", word_idx++, token); 
        }
        strcat(*result, tmp); 
        token = strtok(NULL, "|"); 
    } 
    assert(word_idx == word_per_line);
}

void *threaded_task(void *param) {
    mu.lock();
    Param* p = (Param*) param;

    // size: 20 lines * 64 words
    p->result = (char*) malloc(p->line_per_thread * word_per_line * 64);
    for(size_t i=0;i<p->line_per_thread;i++){
        strcat(p->result, "{\n");
        str_to_json(&p->result, p->line[i]);
        strcat(p->result, "},");
    }
    mu.unlock();

    pthread_exit((void *)param);
}

void get_fp(FILE** fp, const char* name , const char* mode){
    *fp = fopen(name, mode); 
    if (*fp == NULL) {
        printf("Error! %s\n", name);
        exit(1);
    }  
}

void inin_thread_obj(Param* param_obj, int thread_num){
    for(int i=0;i<thread_num;i++){
        param_obj[i].line = new char* [max_line_per_thread];
        for(size_t j=0;j<max_line_per_thread;j++)
            param_obj[i].line[j] = (char*)malloc(line_size*(sizeof(char)));
    }
}

int main(int argc, char* argv[]) {
    const clock_t begin_time = clock();
    assert(argc == 2);
    size_t thread_num = atoi(argv[1]);
    assert(thread_num > 0);

    cout<<"Thread num: "<<argv[1]<<endl;
    FILE *in_fp, *out_fp;
    
    get_fp(&in_fp, "input.csv", "r");
    get_fp(&out_fp, "output.json", "w");

    char line[line_size];
    Param param_obj[thread_num];
    pthread_attr_t attr;
    pthread_t thread[thread_num];
    int rc;
    void *status;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    size_t line_idx = 0;
    size_t thread_idx = 0;

    fprintf(out_fp, "[");

    inin_thread_obj(param_obj, thread_num);

    while (fgets(line, sizeof(line), in_fp)) {
        assert(thread_idx < thread_num);
        memset(param_obj[thread_idx].line[line_idx], 0, line_size);
        strcpy(param_obj[thread_idx].line[line_idx], line);

        line_idx++;
        // it would create a thread for processing n lines, where n=`line_per_thread`
        if(line_idx == max_line_per_thread){
            param_obj[thread_idx].line_per_thread = max_line_per_thread;
            rc = pthread_create(&thread[thread_idx], &attr, threaded_task, (void *)&param_obj[thread_idx]);
            if (rc) {
                printf("ERROR: return code from pthread_create() is %d\n", rc);
                exit(-1);
            }
            thread_idx++;
            line_idx = 0; 
        }

        // If the thread_idx equals to the thread_num, joining all threads and writing the results into the output file.`
        if(thread_idx == thread_num){
            for (size_t t = 0; t < thread_idx; t++) {
                rc = pthread_join(thread[t], &status);
                if (rc) {
                    printf("ERROR; return code from pthread_join() is %d\n", rc);
                    exit(-1);
                }
                fprintf(out_fp, "%s", ((Param*)status)->result);
            }
            pthread_attr_destroy(&attr);
            thread_idx = 0;
            line_idx = 0;
        }
    }

    // Check if there are any remain lines that have not been processed yet.
    if(line_idx != 0){
        param_obj[thread_idx].line_per_thread = line_idx;
        rc = pthread_create(&thread[thread_idx], &attr, threaded_task, (void *)&param_obj[thread_idx]);
        if (rc) {
            printf("ERROR: return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
        thread_idx++;
    }

    // Check if there are any remain threads that have not been joined yet.
    if(thread_idx != 0){
        for (size_t t = 0; t < thread_idx; t++) {
            rc = pthread_join(thread[t], &status);
            if (rc) {
                printf("ERROR; return code from pthread_join() is %d\n", rc);
                exit(-1);
            }
            fprintf(out_fp, "%s", ((Param*)status)->result);
        }
    }
    
    for(size_t i=0;i<thread_num;i++){
        if(param_obj[i].result)
            free(param_obj[i].result);
    }

    // For remove the redundant last comma
    size_t pos = ftell(out_fp);
    fseek( out_fp, pos-1, SEEK_SET );
    fprintf(out_fp, "]");

    fclose(in_fp);
    fclose(out_fp);
    cout <<"Total time: "<< float( clock () - begin_time ) /  CLOCKS_PER_SEC <<" s";
}