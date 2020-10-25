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
size_t limit = 50; 
mutex mu;

typedef struct {
    char** line;
    char* result;
    size_t limit_set;
} Param;

void str_to_json(char** result, char* line){
    size_t ln = strlen(line) - 1;
    if (*line && line[ln] == '\n') 
        line[ln] = '\0';

    int index = 1;
    char* token = strtok(line, "|"); 
    
    while (token != NULL) { 
        if(index != 20){
            char tmp[64];
            sprintf(tmp, "\"col_%d\":\"%s\",\n", index++, token);
            strcat(*result, tmp); 
        }
        else{
            char tmp[64];
            sprintf(tmp, "\"col_%d\":\"%s\"\n", index++, token); 
            strcat(*result, tmp); 
        }
        token = strtok(NULL, "|"); 
    } 
    assert(index == 21);
}

void *threaded_task(void *param) {
    mu.lock();
    Param* p = (Param*) param;

    // 20 lines * 64 = 1 set
    p->result = (char*) malloc(p->limit_set * 20 * 64);
    for(size_t i=0;i<p->limit_set;i++){
        strcat(p->result, "{\n");
        str_to_json(&p->result, p->line[i]);
        strcat(p->result, "},");
    }
    mu.unlock();

    pthread_exit((void *)param);
}

int main(int argc, char* argv[]) {

    const clock_t begin_time = clock();
    char const* const input_file = "input.csv"; 
    char const* const output_file = "output.json";

    assert(argc == 2);
    size_t NUM_THREADS = atoi(argv[1]);
    assert(NUM_THREADS > 0);

    cout<<"Thread num: "<<argv[1]<<endl;

    FILE* in_fp = fopen(input_file, "r"); 
    if (in_fp == NULL) {
        printf("Error! in_fp");
        exit(1);
    }  
    FILE *out_fp;
    out_fp = fopen(output_file, "w");
    if (out_fp == NULL) {
        printf("Error! out_fp");
        exit(1);
    }  

    char line[1024];
    Param param_obj[NUM_THREADS];
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

    // convert now to string form
    fprintf(out_fp, "[");
    while (fgets(line, sizeof(line), in_fp)) {
        assert(t < NUM_THREADS);
        if(new_obj){
            param_obj[t].line = new char* [limit];
            new_obj = false;
        }
        param_obj[t].line[limit_ctr] = (char*)malloc(1024*(sizeof(char)));
        strcpy(param_obj[t].line[limit_ctr], line);
        if(limit_ctr == limit - 1){
            param_obj[t].limit_set = limit;
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

        if(thread_in_use == NUM_THREADS){
            for (t = 0; t < thread_in_use; t++) {
                rc = pthread_join(thread[t], &status);
                if (rc) {
                    printf("ERROR; return code from pthread_join() is %d\n", rc);
                    exit(-1);
                }
                fprintf(out_fp, "%s", ((Param*)status)->result);
            }
            // Handle the last remain datas which don't meet the limit. 
            if(!new_obj){
                param_obj[t].limit_set = limit_ctr;
                rc = pthread_create(&thread[t], &attr, threaded_task, (void *)&param_obj[t]);
                if (rc) {
                    printf("ERROR: return code from pthread_create() is %d\n", rc);
                    exit(-1);
                }
                rc = pthread_join(thread[t], &status);
                if (rc) {
                    printf("ERROR; return code from pthread_join() is %d\n", rc);
                    exit(-1);
                }
                fprintf(out_fp, "%s", ((Param*)status)->result);
            }
            pthread_attr_destroy(&attr);
            thread_in_use = 0;
            limit_ctr = 0;
            new_obj = true;
            thread_in_use = 0;
            t = 0;
        }
    }

    if(thread_in_use != NUM_THREADS){
        for (size_t j = 0; j < thread_in_use; j++) {
            rc = pthread_join(thread[j], &status);
            if (rc) {
                printf("ERROR; return code from pthread_join() is %d\n", rc);
                exit(-1);
            }
            fprintf(out_fp, "%s", ((Param*)status)->result);
        }
    }
    if(!new_obj){

        param_obj[t].limit_set = limit_ctr;
        rc = pthread_create(&thread[t], &attr, threaded_task, (void *)&param_obj[t]);
        if (rc) {
            printf("ERROR: return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
        rc = pthread_join(thread[t], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
        fprintf(out_fp, "%s", ((Param*)status)->result);
    }
    
    for(size_t i=0;i<NUM_THREADS;i++){
        free(param_obj[i].result);
        for(size_t j=0;j<limit;j++)
            free(param_obj[i].line[j]);
    }

    // For remove the redundant last comma
    size_t pos = ftell(out_fp);
    fseek( out_fp, pos-1, SEEK_SET );
    fprintf(out_fp, "]");

    fclose(in_fp);
    fclose(out_fp);
    cout <<"Total time: "<< float( clock () - begin_time ) /  CLOCKS_PER_SEC <<" s";
}