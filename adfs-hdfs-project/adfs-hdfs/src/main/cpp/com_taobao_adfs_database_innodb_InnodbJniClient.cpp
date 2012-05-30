#include "com_taobao_adfs_database_innodb_InnodbJniClient.h"
#include <cstring>

/**
 * javaIncludePath=`which java`; javaIncludePath=`readlink -f $javaIncludePath`; javaIncludePath=`dirname $javaIncludePath`; javaIncludePath=$javaIncludePath/../../include; g++ -shared -I $javaIncludePath com_taobao_adfs_database_innodb_InnodbJniClient.cpp -o libinnodb.so
 */

JNIEXPORT jobject JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_find(JNIEnv *env, jobject object, jstring database, jstring table, jstring index, jobjectArray key, jint comparator, jint limit) {
  return 0;
}

JNIEXPORT jint JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_insert(JNIEnv *env, jobject object, jstring database, jstring table, jobjectArray value) {
  const char *str = env->GetStringUTFChars(database, 0);
  char str2[1024];
  strcpy(str2, str);
  env->ReleaseStringUTFChars(database, str);
  return strlen(str2);
}

JNIEXPORT jint JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_update(JNIEnv *env, jobject object, jstring database, jstring table, jstring index, jobjectArray key, jint comparator, jint limit, jobjectArray value) {
  return 2;
}

JNIEXPORT jint JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_delete(JNIEnv *env, jobject object, jstring database, jstring table, jstring index, jobjectArray key, jint comparator, jint limit) {
  return 3;
}

JNIEXPORT jint JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_open(JNIEnv *env, jobject object) {
  return 4;
}

JNIEXPORT jint JNICALL Java_com_taobao_adfs_database_innodb_InnodbJniClient_close(JNIEnv *env, jobject object) {
  return 5;
}

