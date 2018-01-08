package com.surfilter.Utils

/**
  * Athor: zhouning
  * Description: 
  * Created by on: 2017/9/1
  */
object KmpUtils {

  private[KmpUtils] def getNext(p: Array[Char]): Array[Int] = {

    val pLen = p.length
    val next = new Array[Int](pLen)
    var k = -1
    var j = 0
    next(0) = -1
    while (j < pLen - 1) {
      if (k == -1 || p(j) == p(k)) {
        k += 1
        j += 1

        if (p(j) != p(k)) {
          next(j) = k
        } else {
          next(j) = next(k)
        }
      } else {
        k = next(k)
      }
    }
    next
  }

  def indexOf(source: String, pattern: String): Int = {
    var i = 0
    var j = 0
    val src: Array[Char] = source.toCharArray()
    val ptn: Array[Char] = pattern.toCharArray()
    val sLen = src.length
    val pLen = ptn.length
    val next: Array[Int] = getNext(ptn)
    while (i < sLen && j < pLen) {
      if (j == -1 || src(i) == ptn(j)) {

        i += 1
        j += 1
      } else {
        j = next(j)
      }
    }
    if (j == pLen) {
      i - j
    } else {
      -1
    }
  }

}
