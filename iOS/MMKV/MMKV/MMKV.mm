/*
 * Tencent is pleased to support the open source community by making
 * MMKV available.
 *
 * Copyright (C) 2018 THL A29 Limited, a Tencent company.
 * All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *       https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#import "MMKV.h"
#import "MemoryFile.h"
#import "MiniCodedInputData.h"
#import "MiniCodedOutputData.h"
#import "MiniPBCoder.h"
#import "MiniPBUtility.h"
#import "ScopedLock.hpp"

#ifdef __IPHONE_OS_VERSION_MIN_REQUIRED
#import <UIKit/UIKit.h>
#endif

#import <sys/mman.h>
#import <sys/stat.h>
#import <unistd.h>
#import <zlib.h>

static NSMutableDictionary *g_instanceDic;
static NSRecursiveLock *g_instanceLock;

int DEFAULT_MMAP_SIZE;

#define DEFAULT_MMAP_ID @"mmkv.default"
#define CRC_FILE_SIZE DEFAULT_MMAP_SIZE
#define SPECIAL_CHARACTER_DIRECTORY_NAME @"specialCharacter"


@implementation MMKV {
	NSRecursiveLock *m_lock;
	NSMutableArray *m_arr;
	NSString *m_path;
	NSString *m_mmapID;
	int m_fd;
	char *m_ptr;
	size_t m_size;
	size_t m_actualSize;
    size_t m_elementNum;
	MiniCodedOutputData *m_output;

	BOOL m_isInBackground;
	BOOL m_needLoadFromFile;
    BOOL m_needLoadFileToMemory;
	BOOL m_hasFullWriteBack;

}

#pragma mark - init

+ (void)initialize {
	if (self == MMKV.class) {
		g_instanceDic = [NSMutableDictionary dictionary];
		g_instanceLock = [[NSRecursiveLock alloc] init];

		DEFAULT_MMAP_SIZE = getpagesize() * 2;
		NSLog(@"pagesize:%d", DEFAULT_MMAP_SIZE);
	}
}


+ (instancetype)mmkvWithID:(NSString *)mmapID cryptKey:(NSData *)cryptKey relativePath:(nullable NSString *)relativePath {
	if (mmapID.length <= 0) {
		return nil;
	}

	NSString *kvPath = [MMKV mappedKVPathWithID:mmapID relativePath:relativePath];
	if (!isFileExist(kvPath)) {
		if (!createFile(kvPath)) {
			NSLog(@"fail to create file at %@", kvPath);
			return nil;
		}
	}
	NSString *kvKey = [MMKV mmapKeyWithMMapID:mmapID relativePath:relativePath];

	CScopedLock lock(g_instanceLock);

	MMKV *kv = [g_instanceDic objectForKey:kvKey];
	if (kv == nil) {
		kv = [[MMKV alloc] initWithMMapID:kvKey cryptKey:cryptKey path:kvPath];
		[g_instanceDic setObject:kv forKey:kvKey];
	}
	return kv;
}

- (instancetype)initWithMMapID:(NSString *)kvKey cryptKey:(NSData *)cryptKey path:(NSString *)path {
	if (self = [super init]) {
		m_lock = [[NSRecursiveLock alloc] init];

		m_mmapID = kvKey;
		m_path = path;
        m_needLoadFileToMemory = YES;
		[self loadFromFile];

//#ifdef __IPHONE_OS_VERSION_MIN_REQUIRED
//        auto appState = [UIApplication sharedApplication].applicationState;
//        if (appState == UIApplicationStateBackground) {
//            m_isInBackground = YES;
//        } else {
//            m_isInBackground = NO;
//        }
//        NSLog(@"m_isInBackground:%d, appState:%ld", m_isInBackground, (long) appState);
//
//        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(onMemoryWarning) name:UIApplicationDidReceiveMemoryWarningNotification object:nil];
//        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(didEnterBackground) name:UIApplicationDidEnterBackgroundNotification object:nil];
//        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(didBecomeActive) name:UIApplicationDidBecomeActiveNotification object:nil];
//#endif
	}
	return self;
}

- (void)dealloc {
	CScopedLock lock(m_lock);

	[[NSNotificationCenter defaultCenter] removeObserver:self];

	if (m_ptr != MAP_FAILED && m_ptr != nullptr) {
		munmap(m_ptr, m_size);
		m_ptr = nullptr;
	}
	if (m_fd >= 0) {
		close(m_fd);
		m_fd = -1;
	}
	if (m_output) {
		delete m_output;
		m_output = nullptr;
	}
}

#pragma mark - Application state

- (void)onMemoryWarning {
	CScopedLock lock(m_lock);

	NSLog(@"cleaning on memory warning %@", m_mmapID);

	[self clearMemoryCache];
}

#ifdef __IPHONE_OS_VERSION_MIN_REQUIRED
- (void)didEnterBackground {
	CScopedLock lock(m_lock);

	m_isInBackground = YES;
	NSLog(@"m_isInBackground:%d", m_isInBackground);
}

- (void)didBecomeActive {
	CScopedLock lock(m_lock);

	m_isInBackground = NO;
	NSLog(@"m_isInBackground:%d", m_isInBackground);
}
#endif

//#pragma mark - really dirty work
//
//NSData *decryptBuffer(AESCrypt &crypter, NSData *inputBuffer) {
//    size_t length = inputBuffer.length;
//    NSMutableData *tmp = [NSMutableData dataWithLength:length];
//
//    auto input = (const unsigned char *) inputBuffer.bytes;
//    auto output = (unsigned char *) tmp.mutableBytes;
//    crypter.decrypt(input, output, length);
//
//    return tmp;
//}

- (void)loadFromFile {
    /**
     O_RDONLY 以只读方式打开文件
     O_WRONLY 以只写方式打开文件
     O_RDWR 以可读写方式打开文件. 上述三种旗标是互斥的, 也就是不可同时使用, 但可与下列的旗标利用OR(|)运算符组合.

     O_CREAT 若欲打开的文件不存在则自动建立该文件.
     
     S_IRWXU00700 权限, 代表该文件所有者具有可读、可写及可执行的权限.
     S_IRUSR 或S_IREAD, 00400 权限, 代表该文件所有者具有可读取的权限.
     S_IWUSR 或S_IWRITE, 00200 权限, 代表该文件所有者具有可写入的权限.
     S_IXUSR 或S_IEXEC, 00100 权限, 代表该文件所有者具有可执行的权限.
      int open(const char * pathname, int flags, mode_t mode);
     
     */
	m_fd = open(m_path.UTF8String, O_RDWR, S_IRWXU);
	if (m_fd < 0) {
		NSLog(@"fail to open:%@, %s", m_path, strerror(errno));
	} else {
		m_size = 0;
		struct stat st = {};
		if (fstat(m_fd, &st) != -1) {
			m_size = (size_t) st.st_size;
		}
		// round up to (n * pagesize)
		if (m_size < DEFAULT_MMAP_SIZE || (m_size % DEFAULT_MMAP_SIZE != 0)) {
			m_size = ((m_size / DEFAULT_MMAP_SIZE) + 1) * DEFAULT_MMAP_SIZE;
            /**
             ftruncate()会将参数fd指定的文件大小改为参数length指定的大小。参数fd为已打开的文件描述词，而且必须是以写入模式打开的文件。如果原来的文件大小比参数length大，则超过的部分会被删去
             
             返 回  值：0、-1
             */
			if (ftruncate(m_fd, m_size) != 0) {
				NSLog(@"fail to truncate [%@] to size %zu, %s", m_mmapID, m_size, strerror(errno));
				m_size = (size_t) st.st_size;
				return;
			}
		}
        /**
         void * mmap (void *addr,
                      size_t len,
                      int prot,
                      int flags,
                      int fd,
                      off_t offset);
         addr：映射区的开始地址，设置为0时表示由系统决定映射区的起始地址。
         leng：映射区的长度。//长度单位是 以字节为单位，不足一内存页按一内存页处理
         prot：期望的内存保护标志，不能与文件的打开模式冲突。是以下的某个值，可以通过|运算合理地组合在一起
         PROT_EXEC //页内容可以被执行
         PROT_READ //页内容可以被读取
         PROT_WRITE //页可以被写入
         PROT_NONE //页不可访问
         flags：指定映射对象的类型，映射选项和映射页是否可以共享。它的值可以是一个或者多个以下位的组合体
         fd：有效的文件描述词。一般是由open()函数返回，其值也可以设置为-1，此时需要指定flags参数中的MAP_ANON,表明进行的是匿名映射。
         
         offset：被映射对象内容的起点。

         这里的参数我们要重点关注3个length、prot、flags。
         length代表了我们可以操作的内存大小；
         prot代表我们对文件的操作权限。这里传入了读写权限，而且注意要与open()保持一致，所以open()函数传入了O_RDWR可读写权限；。
         flags要写MAP_FILE|MAP_SHARED,我一开始只写了MAP_FILE,能读，但是不能写。
         
         返回值 m_ptr 就是虚拟内存中的地址
         如果失败，mmap 函数将返回 MAP_FAILED。
         */
		m_ptr = (char *) mmap(nullptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
		if (m_ptr == MAP_FAILED) {
			NSLog(@"fail to mmap [%@], %s", m_mmapID, strerror(errno));
		} else {
			const int offset = pbDoubleSize(0);
			NSData *lenBuffer = [NSData dataWithBytesNoCopy:m_ptr length:pbFixed32Size(0) freeWhenDone:NO];
            NSData *numBuffer = [NSData dataWithBytesNoCopy:m_ptr+pbFixed32Size(0) length:pbFixed32Size(0) freeWhenDone:NO];
			@try {
				m_actualSize = MiniCodedInputData(lenBuffer).readFixed32();
                m_elementNum = MiniCodedInputData(numBuffer).readFixed32();
			} @catch (NSException *exception) {
				NSLog(@"%@", exception);
			}
			NSLog(@"loading [%@] with %zu size in total, file size is %zu", m_mmapID, m_actualSize, m_size);
            NSLog(@"loading [%@] with %zu num in total, file size is %zu", m_mmapID, m_elementNum, m_size);
			if (m_actualSize > 0) {
				bool loadFromFile, needFullWriteback = false;
				if (m_actualSize < m_size && m_actualSize + offset <= m_size) {

                    loadFromFile = true;
				} else {
					NSLog(@"load [%@] error: %zu size in total, file size is %zu", m_mmapID, m_actualSize, m_size);
					loadFromFile = false;
                    needFullWriteback = true;
				}
				if (loadFromFile) {
//                    NSData *inputBuffer = [NSData dataWithBytesNoCopy:m_ptr + offset length:m_actualSize freeWhenDone:NO];
//                    m_arr = [MiniPBCoder decodeContainerOfClass:NSMutableArray.class withValueClass:NSData.class fromData:inputBuffer];
					m_output = new MiniCodedOutputData(m_ptr + offset + m_actualSize, m_size - offset - m_actualSize);
					if (needFullWriteback) {
						[self fullWriteBack];
					}
				} else {
					[self writeActualSize:0];
                    [self writeElementNum:0];
					m_output = new MiniCodedOutputData(m_ptr + offset, m_size - offset);
				}
			} else {
				m_output = new MiniCodedOutputData(m_ptr + offset, m_size - offset);
			}
            NSLog(@"loaded [%@] with %zu values", m_mmapID, (unsigned long) m_arr.count);
		}
	}
	if (m_arr == nil) {
		m_arr = [NSMutableArray array];
	}

	if (![self isFileValid]) {
		NSLog(@"[%@] file not valid", m_mmapID);
	}

	tryResetFileProtection(m_path);
	m_needLoadFromFile = NO;
}

- (void)checkLoadData {
	if (m_needLoadFromFile == NO) {
		return;
	}
	m_needLoadFromFile = NO;
	[self loadFromFile];
}

- (void)lodaDataFromFileToMemory
{
    if (m_needLoadFileToMemory == NO) {
        return;
    }
    [self checkLoadData];
    int offset = pbDoubleSize(0);
    NSData *inputBuffer = [NSData dataWithBytesNoCopy:m_ptr + offset length:m_actualSize freeWhenDone:NO];
    m_arr = [MiniPBCoder decodeContainerOfClass:NSMutableArray.class withValueClass:NSData.class fromData:inputBuffer];
    m_needLoadFileToMemory = NO;
}

- (BOOL)canAppendData:(NSData *)value
{
    if (value == nil) {
        return NO;
    }
    if (![self isFileValid]) {
        return NO;
    }
    return value.length + pbRawVarint32Size((int32_t) value.length) <= m_output->spaceLeft();
}
- (BOOL)appendData:(NSData *)value
{
    if (value == nil) {
        return NO;
    }
    
    NSData *data;
    if ([MiniPBCoder isMiniPBCoderCompatibleObject:value]) {
        data = [MiniPBCoder encodeDataWithObject:value];
    } else
    {
        /*if ([object conformsToProtocol:@protocol(NSCoding)])*/ {
            data = [NSKeyedArchiver archivedDataWithRootObject:value];
        }
    }
    
    return [self setRawData:data forKey:nil];
}

- (NSArray<NSData *> *)getFirstData
{
    return [self subarrayWithLength:1];
}

- (NSArray<NSData *> *)subarrayWithLength:(NSUInteger)length
{
    CScopedLock lock(m_lock);
    [self lodaDataFromFileToMemory];
    
    if (length > m_arr.count) {
        NSLog(@"lenght too large = %lu, arr.count = %lu",length,m_arr.count);
        return nil;
    }
    NSArray *subarray = [m_arr subarrayWithRange:NSMakeRange(0, length)];
    NSMutableArray *data_m = [NSMutableArray array];
    for (NSData *data in subarray) {

        if (data.length > 0) {
            if ([MiniPBCoder isMiniPBCoderCompatibleType:[NSData class]]) {
                NSData *dataCoder = [MiniPBCoder decodeObjectOfClass:[NSData class] fromData:data];
                if (dataCoder) {
                    [data_m addObject:dataCoder];
                } else {
                    [data_m addObject:data];
                }
                
            } else {
                if ([[NSData class] conformsToProtocol:@protocol(NSCoding)]) {
                    NSData *dataCoder =  [NSKeyedUnarchiver unarchiveObjectWithData:data];
                    [data_m addObject:dataCoder];
                }
            }
        }

    }
    return data_m.copy;
}

- (NSArray <NSData *>*)getAllData
{
    NSUInteger count = self.count;
    return [self subarrayWithLength:count];
}

- (BOOL)removeFirstData
{
    if (m_arr.count <= 0) {
        return NO;
    }
    CScopedLock lock(m_lock);
    [self checkLoadData];
    
    [m_arr removeObjectAtIndex:0];
    m_hasFullWriteBack = NO;
    
    //    MMKVInfo(@"remove [%@] %lu keys, %lu remain", m_mmapID, (unsigned long) arrKeys.count, (unsigned long) m_dic.count);
    
    return [self fullWriteBack];
}

- (void)clearAll {
	NSLog(@"cleaning all values [%@]", m_mmapID);

	CScopedLock lock(m_lock);

	if (m_needLoadFromFile) {
		if (remove(m_path.UTF8String) != 0) {
			NSLog(@"fail to remove file %@", m_mmapID);
		}
		m_needLoadFromFile = NO;
		[self loadFromFile];
		return;
	}

	[m_arr removeAllObjects];
	m_hasFullWriteBack = NO;

	if (m_output != nullptr) {
		delete m_output;
	}
	m_output = nullptr;

	if (m_ptr != nullptr && m_ptr != MAP_FAILED) {
		// for truncate
		size_t size = std::min<size_t>(DEFAULT_MMAP_SIZE, m_size);
		memset(m_ptr, 0, size);
		if (msync(m_ptr, size, MS_SYNC) != 0) {
			NSLog(@"fail to msync [%@]:%s", m_mmapID, strerror(errno));
		}
		if (munmap(m_ptr, m_size) != 0) {
			NSLog(@"fail to munmap [%@], %s", m_mmapID, strerror(errno));
		}
	}
	m_ptr = nullptr;

	if (m_fd >= 0) {
		if (m_size != DEFAULT_MMAP_SIZE) {
			NSLog(@"truncating [%@] from %zu to %d", m_mmapID, m_size, DEFAULT_MMAP_SIZE);
			if (ftruncate(m_fd, DEFAULT_MMAP_SIZE) != 0) {
				NSLog(@"fail to truncate [%@] to size %d, %s", m_mmapID, DEFAULT_MMAP_SIZE, strerror(errno));
			}
		}
		if (close(m_fd) != 0) {
			NSLog(@"fail to close [%@], %s", m_mmapID, strerror(errno));
		}
	}
	m_fd = -1;
	m_size = 0;
	m_actualSize = 0;

	[self loadFromFile];
}

- (void)clearMemoryCache {
	CScopedLock lock(m_lock);
    NSLog(@"clearMemoryCache");
	if (m_needLoadFromFile) {
		NSLog(@"ignore %@", m_mmapID);
		return;
	}
	m_needLoadFromFile = YES;

	[m_arr removeAllObjects];
	m_hasFullWriteBack = NO;

	if (m_output != nullptr) {
		delete m_output;
	}
	m_output = nullptr;

	if (m_ptr != nullptr && m_ptr != MAP_FAILED) {
		if (munmap(m_ptr, m_size) != 0) {
			NSLog(@"fail to munmap [%@], %s", m_mmapID, strerror(errno));
		}
	}
	m_ptr = nullptr;

	if (m_fd >= 0) {
		if (close(m_fd) != 0) {
			NSLog(@"fail to close [%@], %s", m_mmapID, strerror(errno));
		}
	}
	m_fd = -1;
	m_size = 0;
	m_actualSize = 0;

}

- (void)close {
	CScopedLock g_lock(g_instanceLock);
	CScopedLock lock(m_lock);
	NSLog(@"closing %@", m_mmapID);

	[self clearMemoryCache];

	[g_instanceDic removeObjectForKey:m_mmapID];
}
/*
- (void)trim {
	CScopedLock lock(m_lock);
	NSLog(@"prepare to trim %@", m_mmapID);

	[self checkLoadData];

	if (m_actualSize == 0) {
		[self clearAll];
		return;
	} else if (m_size <= DEFAULT_MMAP_SIZE) {
		return;
	}

	[self fullWriteBack];
	auto oldSize = m_size;
    constexpr auto offset = pbDoubleSize(0);
	while (m_size > (m_actualSize + offset) * 2) {
		m_size /= 2;
	}
	if (oldSize == m_size) {
		NSLog(@"there's no need to trim %@ with size %zu, actualSize %zu", m_mmapID, m_size, m_actualSize);
		return;
	}

	NSLog(@"trimming %@ from %zu to %zu, actualSize %zu", m_mmapID, oldSize, m_size, m_actualSize);

	if (ftruncate(m_fd, m_size) != 0) {
		NSLog(@"fail to truncate [%@] to size %zu, %s", m_mmapID, m_size, strerror(errno));
		m_size = oldSize;
		return;
	}
	if (munmap(m_ptr, oldSize) != 0) {
		NSLog(@"fail to munmap [%@], %s", m_mmapID, strerror(errno));
	}
	m_ptr = (char *) mmap(m_ptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
	if (m_ptr == MAP_FAILED) {
		NSLog(@"fail to mmap [%@], %s", m_mmapID, strerror(errno));
	}

	delete m_output;
	m_output = new MiniCodedOutputData(m_ptr + pbDoubleSize(0), m_size - pbDoubleSize(0));
	m_output->seek(m_actualSize);

	NSLog(@"finish trim %@ to size %zu", m_mmapID, m_size);
}
*/
- (BOOL)protectFromBackgroundWriting:(size_t)size writeBlock:(void (^)(MiniCodedOutputData *output))block {
	if (m_isInBackground) {
		static const int offset = pbDoubleSize(0);
		static const int pagesize = getpagesize();
		size_t realOffset = offset + m_actualSize - size;
		size_t pageOffset = (realOffset / pagesize) * pagesize;
		size_t pointerOffset = realOffset - pageOffset;
		size_t mmapSize = offset + m_actualSize - pageOffset;
		char *ptr = m_ptr + pageOffset;
		if (mlock(ptr, mmapSize) != 0) {
			NSLog(@"fail to mlock [%@], %s", m_mmapID, strerror(errno));
			// just fail on this condition, otherwise app will crash anyway
			//block(m_output);
			return NO;
		} else {
			@try {
				MiniCodedOutputData output(ptr + pointerOffset, size);
				block(&output);
				m_output->seek(size);
			} @catch (NSException *exception) {
				NSLog(@"%@", exception);
				return NO;
			} @finally {
				munlock(ptr, mmapSize);
			}
		}
	} else {
		block(m_output);
	}

	return YES;
}

// since we use append mode, when -[setData: forKey:] many times, space may not be enough
// try a full rewrite to make space
- (BOOL)ensureMemorySize:(size_t)newSize {
	[self checkLoadData];

	if (![self isFileValid]) {
		NSLog(@"[%@] file not valid", m_mmapID);
		return NO;
	}

	// make some room for placeholder
	constexpr uint32_t /*ItemSizeHolder = 0x00ffffff,*/ ItemSizeHolderSize = 8;
	if (m_elementNum == 0) {
		newSize += ItemSizeHolderSize;
	}
    if (newSize >= m_output->spaceLeft()) return NO;
	if (/*newSize >= m_output->spaceLeft() ||*/ m_elementNum == 0) {
		// try a full rewrite to make space
		static const int offset = pbDoubleSize(0);
        //优化
        NSData *data = [MiniPBCoder encodeDataWithObject:m_arr];
//        size_t lenNeeded = data.length + offset + newSize;
//        size_t avgItemSize = lenNeeded / std::max<size_t>(1, m_arr.count);
//        size_t futureUsage = avgItemSize * std::max<size_t>(8, m_arr.count / 2);
		// 1. no space for a full rewrite, double it
		// 2. or space is not large enough for future usage, double it to avoid frequently full rewrite
        /*
		if (lenNeeded >= m_size || (lenNeeded + futureUsage) >= m_size) {
			size_t oldSize = m_size;
			do {
				m_size *= 2;
			} while (lenNeeded + futureUsage >= m_size);
			NSLog(@"extending [%@] file size from %zu to %zu, incoming size:%zu, future usage:%zu",
			         m_mmapID, oldSize, m_size, newSize, futureUsage);

			// if we can't extend size, rollback to old state
			if (ftruncate(m_fd, m_size) != 0) {
				NSLog(@"fail to truncate [%@] to size %zu, %s", m_mmapID, m_size, strerror(errno));
				m_size = oldSize;
				return NO;
			}

			if (munmap(m_ptr, oldSize) != 0) {
				NSLog(@"fail to munmap [%@], %s", m_mmapID, strerror(errno));
			}
			m_ptr = (char *) mmap(m_ptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
			if (m_ptr == MAP_FAILED) {
				NSLog(@"fail to mmap [%@], %s", m_mmapID, strerror(errno));
			}

			// check if we fail to make more space
			if (![self isFileValid]) {
				NSLog(@"[%@] file not valid", m_mmapID);
				return NO;
			}
			// keep m_output consistent with m_ptr -- writeAcutalSize: may fail
			delete m_output;
			m_output = new MiniCodedOutputData(m_ptr + offset, m_size - offset);
			m_output->seek(m_actualSize);
		}
*/
		if ([self writeActualSize:data.length] == NO) {
			return NO;
		}
        if ([self writeElementNum:m_elementNum] == NO) {
            return NO;
        }
		delete m_output;
		m_output = new MiniCodedOutputData(m_ptr + offset, m_size - offset);
		BOOL ret = [self protectFromBackgroundWriting:m_actualSize
		                                   writeBlock:^(MiniCodedOutputData *output) {
			                                   output->writeRawData(data);
		                                   }];
		return ret;
	}
	return YES;
     
}

- (BOOL)writeElementNum:(size_t)num {
    assert(m_ptr != 0);
    assert(m_ptr != MAP_FAILED);
    
    char *tmpPtr = nullptr;
    static const int offset = pbFixed32Size(0);
    char *actualNumPtr = m_ptr + offset;
    if (m_isInBackground) {
        tmpPtr = m_ptr + offset;
        if (mlock(tmpPtr, offset) != 0) {
            NSLog(@"fail to mmap [%@], %d:%s", m_mmapID, errno, strerror(errno));
            // just fail on this condition, otherwise app will crash anyway
            return NO;
        } else {
            actualNumPtr = tmpPtr;
        }
    }
    
    @try {
        MiniCodedOutputData output(actualNumPtr, offset);
        output.writeFixed32((int32_t) num);
    } @catch (NSException *exception) {
        NSLog(@"%@", exception);
    }
    m_elementNum = num;
    
    if (tmpPtr != nullptr && tmpPtr != MAP_FAILED) {
        munlock(tmpPtr, offset);
    }
    return YES;
}
- (BOOL)writeActualSize:(size_t)actualSize {
	assert(m_ptr != 0);
	assert(m_ptr != MAP_FAILED);

	char *actualSizePtr = m_ptr;
	char *tmpPtr = nullptr;
	static const int offset = pbFixed32Size(0);

	if (m_isInBackground) {
		tmpPtr = m_ptr;
        /**
         mlock机制主要有以下功能：
         a)被锁定的物理内存在被解锁或进程退出前，不会被页回收流程处理。
         b)被锁定的物理内存，不会被交换到swap设备。
         c)进程执行mlock操作时，内核会立刻分配物理内存（注意COW的情况）
         */
		if (mlock(tmpPtr, offset) != 0) {
			NSLog(@"fail to mmap [%@], %d:%s", m_mmapID, errno, strerror(errno));
			// just fail on this condition, otherwise app will crash anyway
			return NO;
		} else {
			actualSizePtr = tmpPtr;
		}
	}

	@try {
		MiniCodedOutputData output(actualSizePtr, offset);
		output.writeFixed32((int32_t) actualSize);
	} @catch (NSException *exception) {
		NSLog(@"%@", exception);
	}
	m_actualSize = actualSize;

	if (tmpPtr != nullptr && tmpPtr != MAP_FAILED) {
		munlock(tmpPtr, offset);
	}
	return YES;
}

- (BOOL)setRawData:(NSData *)data forKey:(NSString *)key {
	if (data.length <= 0 /*|| key.length <= 0*/) {
		return NO;
	}
	CScopedLock lock(m_lock);

	auto ret = [self appendData:data forKey:key];
	if (ret) {
//        [m_arr addObject:data];
		m_hasFullWriteBack = NO;
	}
	return ret;
}

- (BOOL)appendData:(NSData *)data forKey:(NSString *)key {
	auto size = data.length + pbRawVarint32Size((int32_t) data.length); // size needed to encode the value

    BOOL hasEnoughSize = [self ensureMemorySize:size];
    if (hasEnoughSize == NO || [self isFileValid] == NO) {
        return NO;
    }

	BOOL ret = [self writeActualSize:m_actualSize + size];
    BOOL numRet = [self writeElementNum:m_elementNum + 1];
	if (numRet && ret) {
		ret = [self protectFromBackgroundWriting:size
		                              writeBlock:^(MiniCodedOutputData *output) {
			                              output->writeData(data); // note: write size of data
		                              }];
	}
	return ret;
}

- (BOOL)fullWriteBack {
	CScopedLock lock(m_lock);
	if (m_needLoadFromFile) {
		return YES;
	}
	if (m_hasFullWriteBack) {
		return YES;
	}
	if (![self isFileValid]) {
		NSLog(@"[%@] file not valid", m_mmapID);
		return NO;
	}

	if (m_arr.count == 0) {
		[self clearAll];
		return YES;
	}

	NSData *allData = [MiniPBCoder encodeDataWithObject:m_arr];
	if (allData.length > 0) {
		int offset = pbDoubleSize(0);
		if (allData.length + offset <= m_size) {
			BOOL ret = [self writeActualSize:allData.length];
            BOOL numRet = [self writeElementNum:m_arr.count];
			if (numRet && ret) {
				delete m_output;
				m_output = new MiniCodedOutputData(m_ptr + offset, m_size - offset);
				ret = [self protectFromBackgroundWriting:m_actualSize
				                              writeBlock:^(MiniCodedOutputData *output) {
					                              output->writeRawData(allData); // note: don't write size of data
				                              }];
				if (ret) {
					m_hasFullWriteBack = YES;
				}
			}
			return ret;
		} else {
			// ensureMemorySize will extend file & full rewrite, no need to write back again
            // 当前文件的容量不足时,就放弃读写
//            return [self ensureMemorySize:allData.length + offset - m_size];
            return NO;
		}
	}
	return NO;
}

- (BOOL)isFileValid {
	if (m_fd >= 0 && m_size > 0 && m_output != nullptr && m_ptr != nullptr && m_ptr != MAP_FAILED) {
		return YES;
	}
    NSLog(@"[%@] file not valid", m_mmapID);
	return NO;
}


#pragma mark - enumerate



- (size_t)count {
	CScopedLock lock(m_lock);
	[self checkLoadData];
//    return m_arr.count;
    return m_elementNum;
}

- (size_t)totalSize {
	CScopedLock lock(m_lock);
	[self checkLoadData];
	return m_size;
}

- (size_t)actualSize {
	CScopedLock lock(m_lock);
	[self checkLoadData];
	return m_actualSize;
}


#pragma mark - Boring stuff

- (void)sync {
	[self doSync:true];
}

- (void)async {
	[self doSync:false];
}

- (void)doSync:(bool)sync {
	CScopedLock lock(m_lock);
	if (m_needLoadFromFile || ![self isFileValid] /*|| m_crcPtr == nullptr*/) {
		return;
	}

	auto flag = sync ? MS_SYNC : MS_ASYNC;
	if (msync(m_ptr, m_actualSize, flag) != 0) {
		NSLog(@"fail to msync[%d] data file of [%@]:%s", flag, m_mmapID, strerror(errno));
	}
}

static NSString *g_basePath = nil;
+ (NSString *)mmkvBasePath {
	if (g_basePath.length > 0) {
		return g_basePath;
	}

	NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
	NSString *documentPath = (NSString *) [paths firstObject];
	if ([documentPath length] > 0) {
		g_basePath = [documentPath stringByAppendingPathComponent:@"mmkv"];
		return g_basePath;
	} else {
		return @"";
	}
}

+ (void)setMMKVBasePath:(NSString *)basePath {
	if (basePath.length > 0) {
		g_basePath = basePath;
		NSLog(@"set MMKV base path to: %@", g_basePath);
	}
}

+ (NSString *)mmapKeyWithMMapID:(NSString *)mmapID relativePath:(nullable NSString *)relativePath {
	NSString *string = nil;
	if ([relativePath length] > 0 && [relativePath isEqualToString:[MMKV mmkvBasePath]] == NO) {
        string = [relativePath stringByAppendingPathComponent:mmapID];//md5([relativePath stringByAppendingPathComponent:mmapID]);
	} else {
		string = mmapID;
	}
	NSLog(@"mmapKey: %@", string);
	return string;
}

+ (NSString *)mappedKVPathWithID:(NSString *)mmapID relativePath:(nullable NSString *)path {
	NSString *basePath = nil;
	if ([path length] > 0) {
		basePath = path;
	} else {
		basePath = [self mmkvBasePath];
	}

	if ([basePath length] > 0) {
        NSString *mmapIDstring = mmapID;//encodeMmapID(mmapID);
		return [basePath stringByAppendingPathComponent:mmapIDstring];
	} else {
		return @"";
	}
}

@end
