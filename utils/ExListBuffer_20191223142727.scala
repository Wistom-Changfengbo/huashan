package com.wangjia.bigdata.core.utils

import scala.collection.mutable

/**
  * Created by Administrator on 2017/8/21.
  */
class ExListBuffer[A](val list: mutable.ListBuffer[A]) {
    /**
      * 删除指定条件值
      *
      * @param isRemove
      * @return
      */
    def remove(isRemove: A => Boolean): mutable.ListBuffer[A] = {
        var i = 0
        while (i < list.size) {
            if (isRemove(list(i))) {
                list.remove(i)
                i -= 1
            }
            i += 1
        }
        list
    }
}

object ExListBuffer {
    implicit def map2ExListBuffer[A](list: mutable.ListBuffer[A]): ExListBuffer[A] = new ExListBuffer(list)
}
