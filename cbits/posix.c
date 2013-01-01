#include "HsMsync.h"

#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include <sys/mman.h>

int system_io_msync(void *ptr, size_t size, int hs_flags)
{
    int flags = 0;
    if (hs_flags & 1) {
        flags |= MS_SYNC;
    }
    if (hs_flags & 2) {
        flags |= MS_ASYNC;
    }
    if (hs_flags & 4) {
        flags |= MS_INVALIDATE;
    }
    return msync(ptr, size, flags);
}
