#include "strbuf.h"

strbuf*
strbuf_new() {
  strbuf* buf = (strbuf*)malloc(sizeof(strbuf));
  if (buf == NULL) { return NULL; }
  buf->size = 1024;
  buf->offset = 0;
  buf->str = (char*) malloc(sizeof(char) * buf->size);
  return buf;
}

int
strbuf_free(strbuf* buf) {
  if (buf == NULL || buf->str == NULL) { return 0; }
  free(buf->str);
  free(buf);
  return 1;
}

