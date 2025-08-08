// thread_annotations.h
#pragma once

#if defined(__clang__)
#define CAPABILITY(x) __attribute__((capability(x)))
#define DCDN_GUARDED_BY(x) __attribute__((guarded_by(x)))
#else
#define CAPABILITY(x)
#define DCDN_GUARDED_BY(x)
#endif

