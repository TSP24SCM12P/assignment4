#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "storage_mgr.h"

FILE *fptr;

extern void initStorageManager(void)
{
    // Initialising file pointer i.e. storage manager.
    fptr = NULL;
}

extern RC createPageFile(char *fileName)
{
    // Create a new page file fileName.
    // The initial file size should be one page. 4096 bytes
    // This method should fill this single page with '\0' bytes

    // open new file and perform read+write operation
    fptr = fopen(fileName, "w+");
    // check if file opened succesfully
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    // Create a page and fill it with '\0' bytes
    SM_PageHandle page = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));
    memset(page, 0, PAGE_SIZE); // set all bytes in page to '\0'

    // write page bytes into file
    if (fwrite(page, sizeof(char), PAGE_SIZE, fptr) < PAGE_SIZE)
        return RC_WRITE_FAILED;

    free(page);   // dispose memory
    fclose(fptr); // close file

    return RC_OK; // ok
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    fptr = fopen(fileName, "r");

    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    // set metadata of opened file
    fHandle->fileName = fileName; // set filename
    fHandle->curPagePos = 0;      // pointer should be point to 1st page in file

    // struct stat to obtain file size
    struct stat fileinfo;
    // success if 0, else -1
    if (stat(fileName, &fileinfo) != 0)
        return RC_FAILED;

    fHandle->totalNumPages = (fileinfo.st_size / PAGE_SIZE); // may need to handle if file size is not a multiple of 1024

    return RC_OK; // ok
}

extern RC closePageFile(SM_FileHandle *fHandle)
{
    // May need to write the updated metadata into the file before closing the file
    // or clear fHandle
    // or this metadata may be handled differently under each function
    if (fptr != NULL)
        fptr = NULL;
    return RC_OK;
}

extern RC destroyPageFile(char *fileName)
{
    fptr = fopen(fileName, "r");
    // check if file exists and is open
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;
    // fclose(fptr);

    // delete file
    if (remove(fileName) != 0)
        return RC_FAILED;

    return RC_OK;
}

extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check if pageNum is non-negative and is within range of existing pages
    if ((pageNum < 0) || (pageNum > fHandle->totalNumPages))
        return RC_READ_NON_EXISTING_PAGE;

    fptr = fopen(fHandle->fileName, "r");
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    // initiate memPage?
    // memPage = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));

    // set cursor to the page you want to read
    if (fseek(fptr, (PAGE_SIZE * pageNum), SEEK_SET) != 0)
        return RC_ERROR;

    // read 1 page of data from fptr and store it in memory pointed to by memPage
    if (fread(memPage, sizeof(char), PAGE_SIZE, fptr) < PAGE_SIZE)
        return RC_FAILED;

    // update current page number in the metadata
    fHandle->curPagePos = ftell(fptr);
    fclose(fptr);

    return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle)
{
    // May need to handle negative values for curPagePos
    return fHandle->curPagePos;
}

extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check if there exists atleast 1 page
    if (fHandle->totalNumPages < 1)
        return RC_READ_NON_EXISTING_PAGE;

    return readBlock(0, fHandle, memPage);
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // to read previous block, obtain cuurent page position and subtract that by 1 and pass it as paramter to readBlock function

    // check if there exists enough pages (there should be min 2 pages)
    // check if currentPagePos is not the first page or a negative number
    if ((fHandle->totalNumPages < 2) || (fHandle->curPagePos < PAGE_SIZE))
        return RC_READ_NON_EXISTING_PAGE;

    return readBlock((fHandle->curPagePos / PAGE_SIZE) - 1, fHandle, memPage);
}

extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // to read cuurent block obtain current page position and pass it as parameter to readBlock function

    // check if there exists enough pages (there should be atleast 1 page)
    // check if currentPagePos is not a negative number
    if ((fHandle->totalNumPages < 1) || (fHandle->curPagePos < 0))
        return RC_READ_NON_EXISTING_PAGE;

    readBlock((fHandle->curPagePos / PAGE_SIZE), fHandle, memPage);
}

extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // to read next block, obtain cuurent page position and add that by 1 and pass it as paramter to readBlock function

    // check if there exists enough pages (there should be atleast 2 pages)
    // check if currentPagePos is not the last page or a negative number
    if ((fHandle->totalNumPages < 2) || ((fHandle->curPagePos / PAGE_SIZE) + 1) >= (fHandle->totalNumPages) || (fHandle->curPagePos < 0))
        return RC_READ_NON_EXISTING_PAGE;

    return readBlock((fHandle->curPagePos / PAGE_SIZE) + 1, fHandle, memPage);
}

extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // to read last block, obtain total number of pages and subtract that by 1 and pass it as parameter to readBlock function
    // NOTE: reading last block is the same as reading first block if totalNumPages = 1?

    // check if there exists enough pages (there should be atleast 1 page)
    if (fHandle->totalNumPages < 1)
        return RC_READ_NON_EXISTING_PAGE;

    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
    if (pageNum > fHandle->totalNumPages || pageNum < 0)
        return RC_WRITE_FAILED;

    // Opening file stream in read & write mode
    fptr = fopen(fHandle->fileName, "r+");

    // Checking if file was opened succesfully.
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    if (pageNum != 0)
    {
        // Writing data to the first page.
        fHandle->curPagePos = PAGE_SIZE * pageNum;
        fclose(fptr);
        writeCurrentBlock(fHandle, memPage);
    }

    // Check if the pageNum is the first page that needs to be written on
    if (pageNum == 0)
    {
        fseek(fptr, PAGE_SIZE * pageNum, SEEK_SET);

        // Writing the entire page from memPage to page file
        if (fwrite(memPage, sizeof(char), PAGE_SIZE, fptr) < PAGE_SIZE)
        {
            fclose(fptr);
            return RC_WRITE_FAILED;
        }

        // Setting the current page position to the cursor(pointer) position of the file stream
        fHandle->curPagePos = ftell(fptr);

        // Closing file stream so that all the buffers are flushed.
        fclose(fptr);
    }
    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Opening file stream in read & write mode
    fptr = fopen(fHandle->fileName, "r+");

    // Checking if file was opened succesfully.
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    // Appending an empty block ensure there is space for the new content.
    appendEmptyBlock(fHandle);

    // Setting pointer position in file
    fseek(fptr, fHandle->curPagePos, SEEK_SET);

    // Writing memPage contents to the file.
    if(fwrite(memPage, sizeof(char), strlen(memPage), fptr) < PAGE_SIZE){
        fclose(fptr);
        return RC_WRITE_FAILED;
    }

    // Setting the current page position to the cursor(pointer) position of the fHandle
    fHandle->curPagePos = ftell(fptr);

    fclose(fptr);
    return RC_OK;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    // TODO: create new page
    // WARN: not responsible for position where to insert

    // Create a page and fill it with '\0' bytes
    SM_PageHandle page = (SM_PageHandle)malloc(PAGE_SIZE * sizeof(char));
    memset(page, 0, PAGE_SIZE); // set all bytes in page to '\0'

    // set pointer to the newly appended page
    int pos = (fHandle->totalNumPages) * PAGE_SIZE;
    fseek(fptr, pos, SEEK_SET);
    // write page bytes into file
    if (fwrite(page, sizeof(char), PAGE_SIZE, fptr) < PAGE_SIZE)
        return RC_WRITE_FAILED;

    free(page);               // dispose memory
    fHandle->totalNumPages++; // update total pages

    return RC_OK; // ok
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    // Opening file stream in append mode.
    fptr = fopen(fHandle->fileName, "a");

    // Check if file opened successfully
    if (fptr == NULL)
        return RC_FILE_NOT_FOUND;

    // Check if numberOfPages is greater than totalNumPages.
    // If so, add empty pages till numberofPages is equal to the totalNumPages
    for (; numberOfPages > fHandle->totalNumPages;)
        appendEmptyBlock(fHandle);

    // Closing file stream so that all the buffers are flushed.
    fclose(fptr);
    return RC_OK;
}
