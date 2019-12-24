package com.wangjia.bigdata.core.handler

/**
  * Created by Administrator on 2017/9/22.
  */
/**
  * 加载数据
  *
  * @param iTime      更新数据间隔时间(ms)
  * @param updateFunc 更新函数
  * @tparam T 数据类型
  */
class LoadDataHandler[T](val iTime: Long, val updateFunc: () => T)
        extends Serializable {
    private var data: T = updateFunc()
    private var upUpdateTime: Long = System.currentTimeMillis()

    def value(): T = {
        update()
        data
    }

    def update(): Unit = {
        if (System.currentTimeMillis() - upUpdateTime >= iTime) {
            val _data = updateFunc()
            if (_data != null)
                data = _data
            upUpdateTime = System.currentTimeMillis()
        }
    }
}

object LoadDataHandler {
    implicit def handler2Data[T](handler: LoadDataHandler[T]): T = handler.value()
}