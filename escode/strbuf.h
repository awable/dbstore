/*
 * Copyright (C) 2014 Akhil Wable
 * Author: Akhil Wable <awable@gmail.com>
 *
 * String buffer implementation for ESCODE encoder
 *
 */

#ifndef __STRBUF_H__
#define __STRBUF_H__

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

typedef struct strbuf {
  char *str;
  uint32_t size;
  uint32_t offset;
} strbuf;

strbuf* strbuf_new(void);
int strbuf_free(strbuf* buf);

inline int strbuf_put(strbuf* buf, const char* contents, uint32_t size) {
  if (buf == NULL) { return 0; }

  char* old_str = buf->str;
  int required_size = buf->offset + size;

  // Ensure there is enough space in the buffer
  if (buf->size < required_size) {
    int new_size = buf->size * 2;
    if (new_size < required_size) { new_size = required_size; }

    buf->str = (char*) realloc(buf->str, sizeof(char) * new_size);
    if (buf->str == NULL) {
      free(old_str);
      free(buf);
      return 0;
    }

    buf->size = new_size;
  }

  memcpy(buf->str + buf->offset, contents, size);
  buf->offset += size;
  return 1;
}

inline int strbuf_index_put(strbuf* buf,
                            const char* contents,
                            uint32_t size) {

  for (uint32_t i=0; i < size; ++i) {
    char content = *(contents + i);

    if (content == '\xFF') {
      strbuf_put(buf, "\xFF\xFF", sizeof(char) * 2);
    } else {
      content = content + 1;
      strbuf_put(buf, (char*)&content, sizeof(char));
    }
  }

  return 1;
}

#endif //__STRBUF_H__
