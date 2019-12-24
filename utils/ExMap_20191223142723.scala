package com.wangjia.bigdata.core.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/8/17.
  */
class ExMap[A, B](val map: mutable.Map[A, B]) {
    /**
      * 合并MAP
      *
      * @param key
      * @param value
      * @param isUpdate
      */
    def addValue(key: A, value: B, isUpdate: (B, B) => Boolean): mutable.Map[A, B] = {
        if (!map.contains(key)) {
            map.put(key, value)
            return map
        }
        val upVaule: B = map.getOrElse(key, null).asInstanceOf[B]
        if (isUpdate(upVaule, value))
            map.put(key, value)
        map
    }

    def addValue(map2: mutable.Map[A, B], isUpdate: (B, B) => Boolean): mutable.Map[A, B] = {
        for ((k, v) <- map2)
            addValue(k, v, isUpdate)
        map
    }

    /**
      * 删除指定条件值
      *
      * @param isRemove
      * @return
      */
    def remove(isRemove: (A, B) => Boolean): mutable.Map[A, B] = {
        val reKeys = ListBuffer[A]()
        for ((k, v) <- map) {
            if (isRemove(k, v))
                reKeys += k
        }
        reKeys.foreach(k => map.remove(k))
        map
    }
}

object ExMap {
    implicit def map2ExMap[A, B](map: mutable.Map[A, B]): ExMap[A, B] = new ExMap(map)
}
