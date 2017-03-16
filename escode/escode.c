/*
 * Copyright (C) 2014 Akhil Wable
 * Author: Akhil Wable <awable@gmail.com>
 *
 * Fast ESCODE encoder/decoder implementation for Python
 *
 */

#include <Python.h>
#include "strbuf.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <ctype.h>
#include <math.h>
#include <arpa/inet.h>

#define _ESCODE_TYPE_NONE 0
#define _ESCODE_TYPE_BOOL 1
#define _ESCODE_TYPE_INT 2
#define _ESCODE_TYPE_UINT 3
#define _ESCODE_TYPE_LONG 4
#define _ESCODE_TYPE_ULONG 5
#define _ESCODE_TYPE_FLOAT 6
#define _ESCODE_TYPE_STRING 7
#define _ESCODE_TYPE_UNICODE 8
#define _ESCODE_TYPE_LIST 9
#define _ESCODE_TYPE_DICT 10

static PyObject *ESCODE_Error;
static PyObject *ESCODE_EncodeError;
static PyObject *ESCODE_DecodeError;

typedef char byte;
static const byte ESCODE_TYPE_NONE = _ESCODE_TYPE_NONE;
static const byte ESCODE_TYPE_BOOL = _ESCODE_TYPE_BOOL;
static const byte ESCODE_TYPE_INT = _ESCODE_TYPE_INT;
static const byte ESCODE_TYPE_UINT = _ESCODE_TYPE_UINT;
static const byte ESCODE_TYPE_LONG = _ESCODE_TYPE_LONG;
static const byte ESCODE_TYPE_ULONG = _ESCODE_TYPE_ULONG;
static const byte ESCODE_TYPE_FLOAT = _ESCODE_TYPE_FLOAT;
static const byte ESCODE_TYPE_STRING = _ESCODE_TYPE_STRING;
static const byte ESCODE_TYPE_UNICODE = _ESCODE_TYPE_UNICODE;
static const byte ESCODE_TYPE_LIST = _ESCODE_TYPE_LIST;
static const byte ESCODE_TYPE_DICT = _ESCODE_TYPE_DICT;

static const int64_t MIN_USINT = 0;
static const int64_t MAX_USINT = 65535;
static const int64_t MIN_INT = -2147483648;
static const int64_t MAX_INT = 2147483647;
static const int64_t MIN_UINT = 0;
static const int64_t MAX_UINT = 4294967295;

int
encode_index_helper(PyObject *object, strbuf* buf) {
    if (object == Py_None) {
      // Do Nothing
    } else if (object == Py_True) {
      strbuf_index_put(buf, "\x01", sizeof(byte));

    } else if (object == Py_False) {
      strbuf_index_put(buf, "\x00", sizeof(byte));

    } else if (PyInt_CheckExact(object) || PyLong_CheckExact(object)) {
      int64_t val = PyLong_AsLongLong(object);
      if (PyErr_Occurred()) {
        PyErr_SetString(ESCODE_EncodeError, "error encoding integer");
        return 0;
      }

      if ((val >= MIN_INT) && (val <= MAX_INT)) {
        // 32 bit signed int
        int32_t ival =  val & 0xFFFFFFFF;
        ival = ival < 0 ? ~ival : (1 << 31) | ival;
        ival = htonl(ival);
        strbuf_index_put(buf, "\x00", sizeof(byte));
        strbuf_index_put(buf, (byte*)&ival, sizeof(ival));

      } else {
        // big endian
        val = val < 0 ? ~val : (1 << 31) | val;
        val = htonl(val);
        strbuf_index_put(buf, (byte*)&val, sizeof(val));
      }

    } else if (PyFloat_CheckExact(object)) {
      double val = PyFloat_AsDouble(object);
      if (PyErr_Occurred()) {
        PyErr_SetString(ESCODE_EncodeError, "error encoding float");
        return 0;
      }

      strbuf_index_put(buf, (byte*)&val, sizeof(val));

    } else if (PyString_CheckExact(object)) {
      char* str;
      Py_ssize_t _len;
      PyString_AsStringAndSize(object, &str, &_len);
      if (_len > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "string too long to encode");
        return 0;
      }

      uint16_t len = _len & 0xFFFF;
      strbuf_index_put(buf, (byte*)str, len);

    } else if (PyUnicode_CheckExact(object)) {
      PyObject* sobject = PyUnicode_AsEncodedString(object, "utf-8", "strict");
      if (sobject == NULL) { return 0; }

      char* str;
      Py_ssize_t _len;
      PyString_AsStringAndSize(sobject, &str, &_len);
      if (_len > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "ustring too long to encode");
        Py_DECREF(sobject);
        return 0;
      }

      uint16_t len = _len & 0xFFFF;
      strbuf_index_put(buf, (byte*)str, len);
      Py_DECREF(sobject);

    } else {
      PyErr_SetString(ESCODE_EncodeError, "object is not ESCODE index encodable");
      return 0;
    }

    return 1;
}

int
encode_index(PyObject *object, PyObject *open, strbuf* buf) {
  if (PyTuple_CheckExact(object)) {
    Py_ssize_t _len = PyTuple_Size(object);
    if (_len > MAX_USINT) {
      PyErr_SetString(ESCODE_EncodeError, "tuple too long to encode");
      return 0;
    }


    uint16_t len = _len & 0xFFFF;
    for (uint16_t idx = 0; idx < len; ++idx) {
      strbuf_put(buf, "\x00", sizeof(byte));
      if (!encode_index_helper(PyTuple_GetItem(object, idx), buf)) {
        return 0;
      }
    }

    // close the index value with \x00 unless open if true
    if (PyObject_Not(open)) {
      strbuf_put(buf, "\x00", sizeof(byte));
    }

  } else {
    PyErr_SetString(ESCODE_EncodeError, "object is not ESCODE index encodable");
    return 0;
  }

  return 1;
}

int
encode_object(PyObject *object, strbuf* buf) {

    if (object == Py_None) {
      strbuf_put(buf, &ESCODE_TYPE_NONE, sizeof(byte));

    } else if (object == Py_True) {
      strbuf_put(buf, &ESCODE_TYPE_BOOL, sizeof(byte));
      strbuf_put(buf, "\x01", sizeof(byte));

    } else if (object == Py_False) {
      strbuf_put(buf, &ESCODE_TYPE_BOOL, sizeof(byte));
      strbuf_put(buf, "\x00", sizeof(byte));

    } else if (PyInt_CheckExact(object) || PyLong_CheckExact(object)) {
      int64_t val = PyLong_AsLongLong(object);
      if (PyErr_Occurred()) {
        PyErr_Clear(); // Clear the error, we'll try unsigned instead

        uint64_t uint64 = PyLong_AsUnsignedLongLong(object);
        if (PyErr_Occurred()) {
          PyErr_SetString(ESCODE_EncodeError, "error encoding integer");
          return 0;
        }

        // 64 bit unsigned int
        strbuf_put(buf, &ESCODE_TYPE_ULONG, sizeof(byte));
        strbuf_put(buf, (byte*)&uint64, sizeof(uint64_t));

      } else if ((val >= MIN_INT) && (val <= MAX_INT)) {
        // 32 bit signed int
        int32_t ival = val & 0xFFFFFFFF;
        strbuf_put(buf, &ESCODE_TYPE_INT, sizeof(byte));
        strbuf_put(buf, (byte*)&ival, sizeof(int32_t));

      } else if ((val >= MIN_UINT) && (val <= MAX_UINT)) {
        // 32 bit unsigned int
        uint32_t ival = val & 0xFFFFFFFF;
        strbuf_put(buf, &ESCODE_TYPE_UINT, sizeof(byte));
        strbuf_put(buf, (byte*)&ival, sizeof(uint32_t));

      } else {
        // 64 bit signed int
        strbuf_put(buf, &ESCODE_TYPE_LONG, sizeof(byte));
        strbuf_put(buf, (byte*)&val, sizeof(int64_t));
      }

    } else if (PyFloat_CheckExact(object)) {
      double val = PyFloat_AsDouble(object);
      if (PyErr_Occurred()) {
        PyErr_SetString(ESCODE_EncodeError, "error encoding float");
        return 0;
      }

      strbuf_put(buf, &ESCODE_TYPE_FLOAT, sizeof(byte));
      strbuf_put(buf, (byte*)&val, sizeof(double));

    } else if (PyString_CheckExact(object)) {
      char* str;
      Py_ssize_t _len;
      PyString_AsStringAndSize(object, &str, &_len);
      if (_len > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "string too long to encode");
        return 0;
      }

      uint16_t len = _len & 0xFFFF;
      strbuf_put(buf, &ESCODE_TYPE_STRING, sizeof(byte));
      strbuf_put(buf, (byte*)&len, sizeof(uint16_t));
      strbuf_put(buf, (byte*)str, len);

    } else if (PyUnicode_CheckExact(object)) {
      PyObject* sobject = PyUnicode_AsEncodedString(object, "utf-8", "strict");
      if (sobject == NULL) { return 0; }

      char* str;
      Py_ssize_t _len;
      PyString_AsStringAndSize(sobject, &str, &_len);
      if (_len > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "ustring too long to encode");
        Py_DECREF(sobject);
        return 0;
      }

      uint16_t len = _len & 0xFFFF;
      strbuf_put(buf, &ESCODE_TYPE_UNICODE, sizeof(byte));
      strbuf_put(buf, (byte*)&len, sizeof(uint16_t));
      strbuf_put(buf, (byte*)str, len);
      Py_DECREF(sobject);

    } else if (PyList_CheckExact(object)) {
      Py_ssize_t _listlen = PyList_Size(object);
      if (_listlen > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "list too long to encode");
        return 0;
      }

      uint16_t listlen = _listlen & 0xFFFF;
      strbuf_put(buf, &ESCODE_TYPE_LIST, sizeof(byte));
      strbuf_put(buf, (byte*)&listlen, sizeof(uint16_t));
      for (uint16_t idx = 0; idx < listlen; ++idx) {
        if (!encode_object(PyList_GetItem(object, idx), buf)) { return 0; }
      }

    } else if (PyDict_CheckExact(object)) {
      Py_ssize_t _dictlen = PyDict_Size(object);
      if (_dictlen > MAX_USINT) {
        PyErr_SetString(ESCODE_EncodeError, "dict too long to encode");
        return 0;
      }

      uint16_t dictlen = _dictlen & 0xFFFF;
      strbuf_put(buf, &ESCODE_TYPE_DICT, sizeof(byte));
      strbuf_put(buf, (byte*)&dictlen, sizeof(uint16_t));

      PyObject *key, *value;
      Py_ssize_t pos = 0;

      while (PyDict_Next(object, &pos, &key, &value)) {
        if (!encode_object(key, buf)) { return 0; }
        if (!encode_object(value, buf)) { return 0; }
      }

    } else {
        PyErr_SetString(ESCODE_EncodeError, "object is not ESCODE encodable");
        return 0;
    }

    return 1;
}

static inline
uint16_t
decode_len(char** pstr, uint32_t* size) {
    if (*size < sizeof(uint16_t)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return 0;
    }

    uint16_t len = *((uint16_t*)*pstr);
    *pstr += sizeof(uint16_t);
    *size -= sizeof(uint16_t);
    return len;
}

PyObject*
decode_object(char** pstr, uint32_t* size) {
  if (*size < sizeof(byte)) {
    PyErr_SetString(ESCODE_DecodeError, "corrupted string");
    return NULL;
  }

  byte type = **pstr;
  *pstr += sizeof(byte);
  *size -= sizeof(byte);

  switch (type) {

  case _ESCODE_TYPE_NONE: {
    Py_RETURN_NONE;
  }
  case _ESCODE_TYPE_BOOL: {
    if (*size < sizeof(byte)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    byte val = **pstr;
    *pstr += sizeof(byte);
    *size -= sizeof(byte);

    if (val) { Py_RETURN_TRUE; }
    Py_RETURN_FALSE;
  }

  case _ESCODE_TYPE_INT: {

    if (*size < sizeof(int32_t)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    int32_t val = *((int32_t*)*pstr);
    *pstr += sizeof(int32_t);
    *size -= sizeof(int32_t);
    return PyLong_FromLong(val);
  }

  case _ESCODE_TYPE_UINT: {

    if (*size < sizeof(uint32_t)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    uint32_t val = *((uint32_t*)*pstr);
    *pstr += sizeof(uint32_t);
    *size -= sizeof(uint32_t);
    return PyLong_FromUnsignedLong(val);
  }

  case _ESCODE_TYPE_LONG: {

    if (*size < sizeof(int64_t)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    int64_t val = *((int64_t*)*pstr);
    *pstr += sizeof(int64_t);
    *size -= sizeof(int64_t);
    return PyLong_FromLongLong(val);
  }

  case _ESCODE_TYPE_ULONG: {

    if (*size < sizeof(uint64_t)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    uint64_t val = *((uint64_t*)*pstr);
    *pstr += sizeof(uint64_t);
    *size -= sizeof(uint64_t);
    return PyLong_FromUnsignedLongLong(val);
  }

  case _ESCODE_TYPE_FLOAT: {

    if (*size < sizeof(double)) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    double val = *((double*)*pstr);
    *pstr += sizeof(double);
    *size -= sizeof(double);
    return PyFloat_FromDouble(val);
  }

  case _ESCODE_TYPE_STRING: {

    uint16_t len = decode_len(pstr, size);
    if (PyErr_Occurred() || *size < len) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    PyObject* obj = PyString_FromStringAndSize(*pstr, len);
    *pstr += len;
    *size -= len;
    return obj;
  }

  case _ESCODE_TYPE_UNICODE: {

    uint16_t len = decode_len(pstr, size);
    if (PyErr_Occurred() || *size < len) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    PyObject* obj = PyUnicode_Decode(*pstr, len, "utf-8", "strict");
    *pstr += len;
    *size -= len;
    return obj;
  }

  case _ESCODE_TYPE_LIST: {

    uint16_t len = decode_len(pstr, size);
    if (PyErr_Occurred()) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    PyObject* obj = PyList_New(len);
    if (obj == NULL) { return NULL; }

    for (uint16_t idx = 0; idx < len; ++idx) {
      PyObject* elem = decode_object(pstr, size);
      // PyList_SetItem steals the reference even on failure
      if (elem == NULL || PyList_SetItem(obj, idx, elem) < 0) {
        Py_DECREF(obj);
        return NULL;
      }
    }

    return obj;
  }

  case _ESCODE_TYPE_DICT: {

    uint16_t len = decode_len(pstr, size);
    if (PyErr_Occurred()) {
      PyErr_SetString(ESCODE_DecodeError, "corrupted string");
      return NULL;
    }

    PyObject* obj = PyDict_New();
    if (obj == NULL) { return NULL; }

    for (uint16_t idx = 0; idx < len; ++idx) {
      PyObject* key = decode_object(pstr, size);
      PyObject* val = decode_object(pstr, size);
      if (key == NULL || val == NULL || PyDict_SetItem(obj, key, val) < 0) {
        Py_XDECREF(key);
        Py_XDECREF(val);
        Py_DECREF(obj);
        return NULL;
      }

      Py_DECREF(key);
      Py_DECREF(val);
    }

    return obj;
  }
  }

  return NULL;
}

/* Encode object or list into its ESCODE index representation */

static PyObject*
ESCODE_encode_index(PyObject *self, PyObject *args)
{
  strbuf* buf = strbuf_new();
  if (buf == NULL) {
    PyErr_SetString(ESCODE_EncodeError, "Error intializing index encode buffer");
    return NULL;
  }

  PyObject *object;
  PyObject *open = Py_False;

  if (!PyArg_ParseTuple(args, "O|O", &object, &open)) {
    PyErr_SetString(ESCODE_EncodeError, "Error intializing index encode buffer");
  }

  if (!encode_index(object, open, buf)) {
    return NULL;
  }

  PyObject* ret = PyString_FromStringAndSize(buf->str, buf->offset);

  strbuf_free(buf);
  return ret;
}

/* Encode object into its ESCODE representation */

static PyObject*
ESCODE_encode(PyObject *self, PyObject *object)
{
  strbuf* buf = strbuf_new();
  if (buf == NULL) {
    PyErr_SetString(ESCODE_EncodeError, "Error intializing encode buffer");
    return NULL;
  }

  if (!encode_object(object, buf)) {
    return NULL;
  }

  PyObject* ret = PyString_FromStringAndSize(buf->str, buf->offset);

  strbuf_free(buf);
  return ret;
}


/* Decode ESCODE representation into python objects */

static PyObject*
ESCODE_decode(PyObject *self, PyObject *object)
{
  if (!PyString_CheckExact(object)) {
    PyErr_SetString(ESCODE_DecodeError, "Can not decode non-string");
    return NULL;
  }

  char* str;
  Py_ssize_t slen;
  PyString_AsStringAndSize(object, &str, &slen);
  if (slen > MAX_UINT) {
    PyErr_SetString(ESCODE_DecodeError, "string too long to decode");
    return 0;
  }

  uint32_t size = (uint32_t)slen;
  return decode_object(&str, &size);
}


/* List of functions defined in the module */

static PyMethodDef escode_methods[] = {
    {"encode", (PyCFunction)ESCODE_encode,  METH_O,
     PyDoc_STR("encode(object) -> generate the ESCODE representation for object.")},

    {"decode", (PyCFunction)ESCODE_decode,  METH_O,
     PyDoc_STR("decode(string) -> parse the ESCODE representation into python objects\n")},

    {"encode_index", (PyCFunction)ESCODE_encode_index,  METH_VARARGS,
     PyDoc_STR("encode(object) -> generate the ESCODE index representation for object.")},

    {NULL, NULL}  // sentinel
};

PyDoc_STRVAR(module_doc,
"ESCODE binary encoding encoder/decoder module."
);

/* Initialization function for the module (*must* be called initescode) */

PyMODINIT_FUNC
initescode(void)
{
    PyObject *m;

    m = Py_InitModule3("escode", escode_methods, module_doc);
    if (m == NULL)
        return;

    ESCODE_Error = PyErr_NewException("on.Error", NULL, NULL);
    if (ESCODE_Error == NULL)
        return;
    Py_INCREF(ESCODE_Error);
    PyModule_AddObject(m, "Error", ESCODE_Error);

    ESCODE_EncodeError = PyErr_NewException("escode.EncodeError", ESCODE_Error, NULL);
    if (ESCODE_EncodeError == NULL)
        return;
    Py_INCREF(ESCODE_EncodeError);
    PyModule_AddObject(m, "EncodeError", ESCODE_EncodeError);

    ESCODE_DecodeError = PyErr_NewException("escode.DecodeError", ESCODE_Error, NULL);
    if (ESCODE_DecodeError == NULL)
        return;
    Py_INCREF(ESCODE_DecodeError);
    PyModule_AddObject(m, "DecodeError", ESCODE_DecodeError);

    // Module version (the MODULE_VERSION macro is defined by setup.py)
    PyModule_AddStringConstant(m, "__version__", MODULE_VERSION);

}
