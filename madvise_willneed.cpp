#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <vector>
#include <chrono>
using h_clock = std::chrono::high_resolution_clock;

enum ErrorCode{
    OpenError = 1 , 
    MmapError ,
    MadviseError ,
    ReadError , 
    UnmapError
};
int pid , rd_break = 100 , huge_rd_break = 500;
std::vector<size_t> pages_res[2];
bool test_time = 0;

int query_page_fault() {
    char ps_command[100];
    sprintf(ps_command , "ps -o majflt,minflt -p %d" , pid);
    return system(ps_command);
}

int drop_caches() {
    printf("Try to drop page caches...\n");
    return system("echo 3 > /proc/sys/vm/drop_caches");
}

void print_block_size(size_t block_size) {
    static char units[][6] = {"Bytes" , "KB" , "MB" , "GB" , "TB" , "PB"};
    constexpr int units_size = sizeof(units) / sizeof(units[0]);
    int now = 0;
    while(block_size >= 1 << 10 && now < units_size - 1) {
        block_size >>= 10;
        now++;
    }

    printf("Block Size : %d %s\n" , block_size , units[now]);
}

size_t count_page_loaded(void *mm_fd , size_t block_size) {
    size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
    size_t need_pages = block_size / PAGE_SIZE + 2;
    unsigned char vec[need_pages] = {0};
    mincore(mm_fd , block_size , vec);

    size_t ret = 0;
    for(auto i : vec) {
        if(i & 1)
            ret ++;
        // printf("%u" , i);
    }
    printf("Pages are resident in core: %lld\n" , ret);

    return ret;
}

size_t read_lines(void* mm_fd , size_t block_size , std::vector<size_t> &res) {
    char *file = (char*)mm_fd;
    const char* end = file + block_size;

    size_t buf_size = 0 , finished = 0 , all_finished = 0;
    char buffer[8192] = {0} , *nextline = NULL;

    auto before_read = h_clock::now();
    size_t count_cost = 0;

    while(file < end) {
        nextline = strchrnul(file , '\r');
        buf_size = nextline - file;
        strncpy(buffer , file , buf_size);
        buffer[buf_size] = 0;
        file = nextline + 1;
        // printf("%s" , buffer);

        all_finished++;
        if( ++finished >= rd_break) {
            printf("%lld entries, " , all_finished);
            auto before_count = h_clock::now();
            auto cnt = count_page_loaded(mm_fd , block_size);
            auto after_count = h_clock::now();
            count_cost += std::chrono::duration_cast<std::chrono::nanoseconds>(after_count - before_count).count();
            
            if(!test_time)
                res.push_back(cnt);
            finished = 0;
        }
    }

    auto after_read = h_clock::now();
    size_t real_cost = std::chrono::duration_cast<std::chrono::nanoseconds>(after_read - before_read).count() - count_cost;
    
    if(test_time) {
        printf("read lines cost: %lld us, mincore cost: %lld us\n", real_cost , count_cost);
        res.push_back(real_cost);
    }

    printf("read entries : %lld, average size: %lf Bytes\n" , all_finished , 1.0 * block_size / all_finished);
    return all_finished;
}

size_t read_huge_blocks(void* mm_fd , size_t block_size , std::vector<size_t> &res) {
    static std::vector<int> each_buf_size;

    char *file = (char*)mm_fd;
    const char* end = file + block_size;

    size_t buf_size = 0 , finished = 0 , all_finished = 0;
    char buffer[1 << 20] = {0} , *nextline = NULL;

    auto before_read = h_clock::now();
    size_t count_cost = 0;

    if(each_buf_size.empty()) {
        while(file < end) {
            buf_size = rand() % (64 << 10) + rand() % (16 << 10) + (6 << 10);
            if(buf_size + file >= end)
                buf_size = end - file;
            strncpy(buffer , file , buf_size);
            buffer[buf_size] = 0;
            file += buf_size;
            each_buf_size.push_back(buf_size);
            // printf("%s" , buffer);

            all_finished++;
            if( ++finished >= huge_rd_break) {
                printf("%lld entries, " , all_finished);
                auto before_count = h_clock::now();
                auto cnt = count_page_loaded(mm_fd , block_size);
                auto after_count = h_clock::now();
                count_cost += std::chrono::duration_cast<std::chrono::microseconds>(after_count - before_count).count();
                res.push_back(cnt);
                finished = 0;
            }
        }
    } else {
        for(auto i : each_buf_size) {
            strncpy(buffer , file , i);
            buffer[i] = 0;
            file += i;
            // printf("%s" , buffer);

            all_finished++;
            if( ++finished >= huge_rd_break) {
                printf("%lld entries, " , all_finished);
                auto before_count = h_clock::now();
                auto cnt = count_page_loaded(mm_fd , block_size);
                auto after_count = h_clock::now();
                count_cost += std::chrono::duration_cast<std::chrono::microseconds>(after_count - before_count).count();
                res.push_back(cnt);
                finished = 0;
            }
        }
    }

    auto after_read = h_clock::now();
    size_t real_cost = std::chrono::duration_cast<std::chrono::microseconds>(after_read - before_read).count() - count_cost;
    printf("read lines cost: %lld us, mincore cost: %lld us\n", real_cost , count_cost);

    printf("read entries : %lld, average size: %lf Bytes\n" , all_finished , 1.0 * block_size / all_finished);
    return all_finished;
}

int mmap_with_madvise(const char *file_name , size_t block_size , size_t (*read_strategy)(void* , size_t , std::vector<size_t>&) = read_lines) {
    print_block_size(block_size);

    int fd = open(file_name , O_RDONLY);
    if(-1 == fd) {
        fprintf(stderr , "Open Error\n");
        return ErrorCode::OpenError;
    }

    void *mm_fd = mmap(NULL , block_size , PROT_READ , MAP_SHARED, fd , 0);
    if(MAP_FAILED == mm_fd) {
        fprintf(stderr , "Mmap Error\n");
        return ErrorCode::MmapError;
    }
    
    if (-1==madvise(mm_fd , block_size , MADV_WILLNEED)) {
        fprintf(stderr , "Madvise error");
        return ErrorCode::MadviseError;
    }
    close(fd);
    fd = -1;

    auto pb = count_page_loaded(mm_fd , block_size);
    // pages_res[0].push_back(pb);
    printf("Ready to read data...\n");

    auto interval = read_strategy(mm_fd , block_size , pages_res[0]);
    if(-1 == interval) {
        fprintf(stderr , "Read Error\n");
        return ErrorCode::ReadError;
    }
    auto pe = count_page_loaded(mm_fd , block_size);
    // pages_res[0].push_back(pe);
    // printf("mmap with madvise time cost: %d ms\n", interval);
    

    if(-1 == munmap(mm_fd , block_size)) {
        fprintf(stderr , "Unmap Error\n");
        return ErrorCode::UnmapError;
    }
    
    query_page_fault();
    return 0;
}

int mmap_without_madvise(const char *file_name , size_t block_size , size_t (*read_strategy)(void* , size_t , std::vector<size_t>&) = read_lines) {
    print_block_size(block_size);

    int fd = open(file_name , O_RDONLY);
    if(-1 == fd) {
        fprintf(stderr , "Open Error\n");
        return ErrorCode::OpenError;
    }

    void *mm_fd = mmap(NULL , block_size , PROT_READ , MAP_SHARED, fd , 0);
    if(MAP_FAILED == mm_fd) {
        fprintf(stderr , "Mmap Error\n");
        return ErrorCode::MmapError;
    }
    close(fd);
    fd = -1;

    /*
    if (-1==madvise(mm_fd , block_size , MADV_RANDOM)) {
        fprintf(stderr , "Madvise error");
        return ErrorCode::MadviseError;
    }*/

    auto pb = count_page_loaded(mm_fd , block_size);
    // pages_res[1].push_back(pb);
    printf("Ready to read data...\n");

    printf("DONT USE MADVISE_WILLNEED!\n");
    int interval = read_strategy(mm_fd , block_size , pages_res[1]);
    if(-1 == interval) {
        fprintf(stderr , "Read Error\n");
        return ErrorCode::ReadError;
    }
    auto pe = count_page_loaded(mm_fd , block_size);
    // pages_res[1].push_back(pe);
    // printf("mmap with madvise time cost: %d ms\n", interval);
    

    if(-1 == munmap(mm_fd , block_size)) {
        fprintf(stderr , "Unmap Error\n");
        return ErrorCode::UnmapError;
    }
    
    query_page_fault();
    return 0;
}

#if 0
void time_cost_test(const char* file_name , size_t block_size)
{
    std::vector<std::pair<double , double>> st;
    test_time = 1;
        
        for(int KK = 0 ; KK < 10 ; KK++) {
            
            constexpr int T = 10;
            for(int t = 0 ; t < T ; t++) {

                printf("\n");
                drop_caches();
                mmap_without_madvise(file_name , block_size);


                printf("\n\n");
                drop_caches();
                mmap_with_madvise(file_name , block_size);
                printf("\n\n");
                
                
            }

            size_t avg_cost[2] = {0};
            for(int i = 0 ; i < 2 ; i++)
            {
                for(auto j : pages_res[i])
                    avg_cost[i] += j;
                pages_res[i].clear();
            }
            
            st.emplace_back((double)avg_cost[0] / T , (double)avg_cost[1] / T);
            printf("Average read time, with madvise: %.2lf ns, without madvise: %.2lf ns\n" , (double)avg_cost[0] / T , (double)avg_cost[1] / T);
        }

        for(auto i : st)
            printf("Average read time, with madvise: %.2lf ns, without madvise: %.2lf ns\n" , i.first , i.second);
}
#endif


int main(int argc , char** argv) {
    srand((unsigned)time(NULL));
    const char file_name[] = "lv3_101.csv";
    size_t block_size = (size_t)(640) << 10;

    pid = getpid();
    {
        printf("\n");
        drop_caches();
        mmap_with_madvise(file_name , block_size);
        printf("\n\n");
        drop_caches();
        mmap_with_madvise(file_name , block_size);
        printf("\n\n");

        for(int i = 0 ; i < pages_res[0].size() ; i++)
            printf("%d Entries, with madvise: %lld, without madvise: %lld\n" , i * rd_break , pages_res[0][i] , pages_res[1][i]);
    }

    return 0;
}


