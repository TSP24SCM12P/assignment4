#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"

/*
 * buffer_mgr.h BM_PageHandle has missing
 * We are defining a new structure for a Page frame in buffer pool and
 * adding required bit flags
 */
typedef struct Frame
{
	PageNumber pgNum;	   // Page number in buffer pool
	SM_PageHandle content; // Holds content of page

	// Flags
	int dirtyFlag; // Indicate modified page
	int fixCnt;	   // No. of client utilizing the page
	int recentCnt; // flag for LRU
} Frame;

// Global Variables
int buff_size = 0, rear = 0, writeCnt = 0, hit = 0;

Frame *frame;	  // declaring frame pointer
SM_FileHandle fh; // declaring filehandle variable
RC code;		  // return code handler

// Memory allocations to free after shutting down buffer
Frame *newFrame;
bool *dirtyFlags;
PageNumber *frameContents;
PageNumber *fixCounts;

// Function Declarations for Page Replacement Strategy
RC FIFO(BM_BufferPool *const, Frame *);
RC LRU(BM_BufferPool *const, Frame *);

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
				  const int numPages, ReplacementStrategy strategy,
				  void *stratData)
{
	(*bm).pageFile = (char *)pageFileName; // set name of the file the buffer pool is associated with
	(*bm).numPages = numPages;			   // Number of pages in the buffer pool
	(*bm).strategy = strategy;			   // Page Replacement Strategy employed for the buffer pool

	frame = calloc(sizeof(Frame), numPages); // Allocate memory for the page Frames in buffer pool

	buff_size = numPages; // Number of frames in the buffer pool

	// Initializing Frame variables
	int i = 0;
	while (i < buff_size)
	{
		frame[i].content = NULL; // empty content
		frame[i].pgNum = -1;	 // set every frame to -1, indicating it is vacant

		// Flags
		frame[i].dirtyFlag = 0; // No modified page present
		frame[i].fixCnt = 0;	// no pages loaded yet to be used
		frame[i].recentCnt = 0; // LRU flag set to 0
		i++;
	}

	(*bm).mgmtData = frame;
	writeCnt = 0; // Set number of write operations to 0
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{

	frame = (Frame *)(*bm).mgmtData;
	code = RC_OK;

	// update altered page Frames to page file on disk if dirty
	if (code = forceFlushPool(bm) != RC_OK)
		return code;

	// Check if there are no pages being utilized by any user
	for (int i = 0; i < buff_size; i++)
	{
		if (frame[i].fixCnt != 0)
			return RC_PINNED_PAGES_IN_BUFFER;
	}

	free(frame);
	(*bm).mgmtData = NULL;
	return code;
}

RC forceFlushPool(BM_BufferPool *const bm)
{

	frame = (Frame *)(*bm).mgmtData;
	code = RC_OK;

	// Push contents of modified page Frames (Dirty) to page file on disk
	for (int i = 0; i < buff_size; i++)
	{
		// Check no user is using the page
		// Check if page Frame has been modified
		if (frame[i].fixCnt == 0 && frame[i].dirtyFlag == DIRTY)
		{
			// Open pageFile on disk
			openPageFile((*bm).pageFile, &fh);
			// write contents from page Frame on buffer pool to page File on disk
			writeBlock(frame[i].pgNum, &fh, frame[i].content);
			frame[i].dirtyFlag = 0;	 // release dirty flag
			writeCnt = writeCnt + 1; // write operation performed into disk
		}
	}
	return code;
}

// Buffer Manager Interface Access Pages
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{

	frame = (Frame *)(*bm).mgmtData;

	int i;
	for (i = 0; i < buff_size; i++)
	{
		// If the current page is the page to be marked dirty, then set dirtyBit = 1 (page has been modified) for that page
		if (frame[i].pgNum == (*page).pageNum)
		{
			frame[i].dirtyFlag = DIRTY;
			return RC_OK;
		}
	}
	return RC_FAILED;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{

	frame = (Frame *)(*bm).mgmtData;

	int i = 0;
	while (i < buff_size)
	{
		// find requested page Number in the buffer pool
		if (frame[i].pgNum == (*page).pageNum)
		{
			// Client no longer is using the page
			frame[i].fixCnt = frame[i].fixCnt - 1; // decrease fix count
			break;
		}
		i++;
	}
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{

	frame = (Frame *)(*bm).mgmtData;
	code = RC_OK;

	int i;
	for (i = 0; i < buff_size; i++)
	{
		// find requested page Number in the buffer pool
		if (frame[i].pgNum == (*page).pageNum)
		{
			// Open pageFile on disk
			if (code = openPageFile((*bm).pageFile, &fh) != RC_OK)
				return code;
			// write contents from page Frame on buffer pool to page File on disk
			if (code = writeBlock(frame[i].pgNum, &fh, frame[i].content) != RC_OK)
				return code;

			frame[i].dirtyFlag = 0; // modified frame already written into disk
			writeCnt = writeCnt + 1;
		}
	}
	return code;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
		   const PageNumber pageNum)
{
	frame = (Frame *)(*bm).mgmtData;
	code = RC_OK;

	// Check if 1st page frame is vacant
	if (frame[0].pgNum == NO_PAGE)
	{
		// Opening page File on disk
		if (code = openPageFile((*bm).pageFile, &fh) != RC_OK)
			return code;
		// Allocating space for the page Frame in the Buffer Pool
		frame[0].content = (SM_PageHandle)malloc(PAGE_SIZE);
		// Reading contents from page File in disk and load it in page Frame of Buffer Pool
		if (code = readBlock(pageNum, &fh, frame[0].content) != RC_OK)
			return code;
		// set the page Frame number to the page File number in the disk
		frame[0].pgNum = pageNum;
		frame[0].fixCnt++;
		// Init LRU params
		rear = hit = 0;
		frame[0].recentCnt = hit;
		(*page).pageNum = pageNum;
		(*page).data = frame[0].content;
		return code;
	}
	else
	{
		int i;
		bool isBufferFull = true;

		for (i = 0; i < buff_size; i++)
		{
			if (frame[i].pgNum != NO_PAGE)
			{
				if (frame[i].pgNum == pageNum)
				{
					frame[i].fixCnt++;
					isBufferFull = false;
					hit++;

					if ((*bm).strategy == RS_LRU)
						// LRU algorithm
						frame[i].recentCnt = hit;
					(*page).pageNum = pageNum;
					(*page).data = frame[i].content;
					break;
				}
			}
			else
			{

				openPageFile((*bm).pageFile, &fh);
				frame[i].content = (SM_PageHandle)malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, frame[i].content);
				frame[i].pgNum = pageNum;
				frame[i].fixCnt = 1;
				rear++;
				hit++;

				if ((*bm).strategy == RS_LRU)
					frame[i].recentCnt = hit;

				(*page).pageNum = pageNum;
				(*page).data = frame[i].content;

				isBufferFull = false;
				break;
			}
		}

		if (isBufferFull)
		{
			// Create a new page to store data read from the file.
			newFrame = (Frame *)malloc(sizeof(Frame));

			// Reading page from disk and initializing page frame's content in the buffer pool
			openPageFile((*bm).pageFile, &fh);
			(*newFrame).content = (SM_PageHandle)malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, (*newFrame).content);
			(*newFrame).pgNum = pageNum;
			(*newFrame).dirtyFlag = 0;
			(*newFrame).fixCnt = 1;
			rear++;
			hit++;

			if ((*bm).strategy == RS_LRU)
				(*newFrame).recentCnt = hit;

			(*page).pageNum = pageNum;
			(*page).data = (*newFrame).content;

			// Call appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)

			// Using FIFO algorithm
			if ((*bm).strategy == RS_FIFO)
			{
				code = FIFO(bm, newFrame);
				if (code != RC_OK)
					return code;
			}
			// Using LRU algorithm
			else if ((*bm).strategy == RS_LRU)
			{

				code = LRU(bm, newFrame);
				if (code != RC_OK)
					return code;
			}
			else
				printf("\n undefined implementation \n");
		}
		return code;
	}
}

// Statistics Interface
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	frameContents = calloc(sizeof(PageNumber), buff_size);
	frame = (Frame *)(*bm).mgmtData;
	// Iterating through all the pages in the buffer pool and setting frameContents' value to pageNum of the page
	for (int i = 0; i < buff_size; i++)
	{

		if (frame[i].pgNum != NO_PAGE)
			frameContents[i] = frame[i].pgNum;
		else
			frameContents[i] = NO_PAGE;
	}
	return frameContents;
}

bool *getDirtyFlags(BM_BufferPool *const bm)
{

	dirtyFlags = calloc(sizeof(bool), buff_size);
	frame = (Frame *)(*bm).mgmtData;

	int i;
	for (i = 0; i < buff_size; i++)
	{
		if (frame[i].dirtyFlag == DIRTY)
			dirtyFlags[i] = true;
		else
			dirtyFlags[i] = false;
	}
	return dirtyFlags;
}

int *getFixCounts(BM_BufferPool *const bm)
{
	fixCounts = calloc(sizeof(int), buff_size);
	frame = (Frame *)(*bm).mgmtData;

	int i = 0;
	while (i < buff_size)
	{
		if (frame[i].fixCnt != -1)
			fixCounts[i] = frame[i].fixCnt;
		else
			fixCounts[i] = 0;
		i++;
	}
	return fixCounts;
}
int getNumReadIO(BM_BufferPool *const bm)
{
	return rear + 1;
}
int getNumWriteIO(BM_BufferPool *const bm)
{
	return writeCnt;
}

/*
============================================================
============================================================
============ PAGE REPLACEMENT IMPLEMENTATION ===============
============================================================
============================================================
*/

// Implementing First-In-First-Out page replacement strategy
RC FIFO(BM_BufferPool *const bm, Frame *page)
{
	// initializing empty frame
	frame = (Frame *)(*bm).mgmtData;
	code = RC_OK;

	int front = rear % buff_size;
	// iterating through every page frame in the buffer pool
	for (int i = 0; i < buff_size; i++)
	{
		// check if page can be evicted
		if (frame[front].fixCnt == 0)
		{
			// check if page frame content has been altered
			if (frame[front].dirtyFlag == DIRTY)
			{
				// move contents from page frame to page file
				if (code = openPageFile((*bm).pageFile, &fh) != RC_OK)
					return code;

				if (code = writeBlock(frame[front].pgNum, &fh, frame[front].content) != RC_OK)
					return code;
				// record write operation
				writeCnt++;
			}

			// loading content from page file to page frame
			frame[front].content = (*page).content;		// load content
			frame[front].pgNum = (*page).pgNum;			// page number being loaded from disk
			frame[front].dirtyFlag = (*page).dirtyFlag; // Initialize dirtFlag to 0
			frame[front].fixCnt = (*page).fixCnt;		// setting fixCnt to 0
			break;
		}

		front++;

		// Loop index 'front' back to the first frame after reaching the end of the frame
		if (front % buff_size == 0)
			front = 0;
	}

	return code;
}

// Implementing Least Recently Used page replacement strategy
RC LRU(BM_BufferPool *const bm, Frame *page)
{
	frame = (Frame *)bm->mgmtData;
	int i, least_recent_index, least_recent_count;
	code = RC_OK;

	for (i = 0; i < buff_size; i++)
	{
		if (frame[i].fixCnt == 0)
		{
			least_recent_index = i;
			least_recent_count = frame[i].recentCnt;
			break;
		}
	}

	// Finding the page frame having minimum recentCnt
	for (i = least_recent_index + 1; i < buff_size; i++)
	{
		if (frame[i].recentCnt < least_recent_count)
		{
			least_recent_index = i;
			least_recent_count = frame[i].recentCnt;
		}
	}

	if (frame[least_recent_index].dirtyFlag == 1)
	{
		SM_FileHandle fh;
		if (code = openPageFile(bm->pageFile, &fh) != RC_OK)
			return code;
		if (code = writeBlock(frame[least_recent_index].pgNum, &fh, frame[least_recent_index].content))
			return code;

		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCnt++;
	}

	// Setting page frame's content to new page's content
	frame[least_recent_index].content = page->content;
	frame[least_recent_index].pgNum = page->pgNum;
	frame[least_recent_index].dirtyFlag = page->dirtyFlag;
	frame[least_recent_index].fixCnt = page->fixCnt;
	frame[least_recent_index].recentCnt = page->recentCnt;
}
