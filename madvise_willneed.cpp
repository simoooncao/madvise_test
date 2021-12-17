#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <chrono>
using h_clock = std::chrono::high_resolution_clock;

enum ErrorCode{
    OpenError = 1 , 
    MmapError ,
    MadviseError ,
    ReadError , 
    UnmapError
};

int  read_lines(char *file , const char *end)
{
    size_t buf_size = 0 , finished = 0;
    char buffer[8192] = {0} , *nextline = NULL;

    while(file < end && finished < 172)
    {
        nextline = strchrnul(file , '\r');
        buf_size = nextline - file;
        strncpy(buffer , file , buf_size);
        buffer[buf_size] = 0;
        file = nextline + 1;
        // printf("%s" , buffer);
        finished++;
    }
    printf("read : %lld\n" , finished);
    return 0;
}

int mmap_with_madvise(const char *file_name)
{
    size_t file_block_size = (size_t)(1) << 16;


    int fd = open(file_name , O_RDONLY);
    if(-1 == fd)
    {
        fprintf(stderr , "Open Error\n");
        return ErrorCode::OpenError;
    }

    size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
    size_t need_pages = file_block_size / PAGE_SIZE + 2;
    unsigned char vec[need_pages] = {0};


    
    void *mm_fd = mmap(NULL , file_block_size , PROT_READ , MAP_SHARED, fd , 0);
    if(MAP_FAILED == mm_fd)
    {
        fprintf(stderr , "Mmap Error\n");
        return ErrorCode::MmapError;
    }
    close(fd);
    fd = -1;

    mincore(mm_fd , file_block_size , vec);
    int before_madvise_page_resident = 0;
    for(auto i : vec)
        if(i & 1)
            before_madvise_page_resident ++;
    printf("Pages are resident in core before madvise with MADV_WILLNEED: %lld\n" , before_madvise_page_resident);


    auto before_madvise = h_clock::now();
    if (-1==madvise(mm_fd , file_block_size , MADV_WILLNEED))
    {
        fprintf(stderr , "Madvise error");
        return ErrorCode::MadviseError;
    }
    auto after_madvise = h_clock::now();
    size_t madvise_cost = std::chrono::duration_cast<std::chrono::nanoseconds>(after_madvise - before_madvise).count();
    printf("madvise call cost: %lld ns\n", madvise_cost);


    memset(vec , 0 , sizeof(vec));
    mincore(mm_fd , file_block_size , vec);
    int after_madvise_page_resident = 0;
    for(auto i : vec)
    {
        if(i & 1)
            after_madvise_page_resident ++;
        printf("%u" , i);
    }
    printf("\nPages are resident in core after madvise with MADV_WILLNEED: %lld\n" , after_madvise_page_resident);


    
    int interval = read_lines((char*)mm_fd , ((char*)mm_fd) + file_block_size);
    if(-1 == interval)
    {
        fprintf(stderr , "Read Error\n");
        return ErrorCode::ReadError;
    }
    // printf("mmap with madvise time cost: %d ms\n", interval);


    memset(vec , 0 , sizeof(vec));
    mincore(mm_fd , file_block_size , vec);
    int after_read_page_resident = 0;
    for(auto i : vec)
    {
        if(i & 1)
            after_read_page_resident ++;
        printf("%u" , i);
    }
    printf("\nPages are resident in core after madvise with MADV_WILLNEED: %lld\n" , after_read_page_resident);
    

    if(-1 == munmap(mm_fd , file_block_size))
    {
        fprintf(stderr , "Unmap Error\n");
        return ErrorCode::UnmapError;
    }
    return 0;
}

int main()
{
    return mmap_with_madvise("lv3_101.csv");
}
