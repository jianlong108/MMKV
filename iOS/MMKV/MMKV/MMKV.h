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

#import "MMKVHandler.h"

@interface MMKV : NSObject

NS_ASSUME_NONNULL_BEGIN


// mmapID: any unique ID (com.tencent.xin.pay, etc)
// if you want a per-user mmkv, you could merge user-id within mmapID
// cryptKey: 16 byte at most
// relativePath: custom path of the file, `NSDocumentDirectory/mmkv` by default
+ (nullable instancetype)mmkvWithID:(NSString *)mmapID cryptKey:(nullable NSData *)cryptKey relativePath:(nullable NSString *)path NS_SWIFT_NAME(init(mmapID:cryptKey:relativePath:));

// clang-format on

// default to `NSDocumentDirectory/mmkv`
+ (NSString *)mmkvBasePath;

// if you want to change the base path, do it BEFORE getting any MMKV instance
// otherwise the behavior is undefined
+ (void)setMMKVBasePath:(NSString *)basePath;
- (BOOL)appendData:(NSData *)value;
- (NSData *)getFirstData;
- (NSArray <NSData *>*)getAllData;
- (NSArray<NSData *> *)subarrayWithLength:(NSUInteger)length;
- (BOOL)removeFirstData;

- (size_t)count;

- (size_t)totalSize;

- (size_t)actualSize;

- (void)clearAll;

// MMKV's size won't reduce after deleting key-values
// call this method after lots of deleting f you care about disk usage
// note that `clearAll` has the similar effect of `trim`
- (void)trim;

// call this method if the instance is no longer needed in the near future
// any subsequent call to the instance is undefined behavior
- (void)close;

// call this method if you are facing memory-warning
// any subsequent call to the instance will load all key-values from file again
- (void)clearMemoryCache;

// you don't need to call this, really, I mean it
// unless you care about out of battery
- (void)sync;
- (void)async;

NS_ASSUME_NONNULL_END

@end
