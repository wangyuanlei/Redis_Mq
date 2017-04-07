<?php
/**
 * 基于redis消息队列
 * Created by lni_wang.
 * Product: PhpStorm
 * Date: 2017/4/6 13:38
 * File: Redis_mq.php
 */
class Redis_mq{
    private $redis = null;
    function __construct($config)
    {
        $this->redis = new Redis();
        $this->redis->pconnect($config['host'], $config['port']);
        $this->redis->select($config['database']);
    }

    /**
     * 发布实时队列消息
     * @param $channel
     * @param $data
     *      model       #需要执行的类
     *      function    #执行的函数名称
     *      params      #需要的参数
     */
    function real_publish($channel, array $data)
    {
        $channel = 'MQ_'.$channel;
        $result = $this->redis->publish($channel, json_encode($data));
        if($result == 0){#当没有客户端接受消息的时候, 暂时保存数据
            $this->redis->lPush($channel, json_encode($data));
        }
    }

    /**
     * 接受订阅消息
     * @param $channel
     * @param $callback
     */
    function subscribe($channel, $function)
    {
        try{
            $channel = 'MQ_'.$channel;
            $count = $this->redis->lSize($channel);
            if($count){
                for($i=$count; $i>0; $i--){
                    $item = $this->redis->lPop($channel);
                    $function(null, null, $item);
                }
            }
            $this->redis->subscribe([$channel], $function);
        }catch (RedisException $e){
            echo 'redis subscribe error'."\r\n";
        }


    }


    /**
     * 发布定时队列消息
     * @param $channel
     * @param $date     #日期 2017-02-11 12:15:13
     * @param $data
     *      model       #需要执行的类
     *      function    #执行的函数名称
     *      params      #需要的参数
     */
    function time_publish($channel, $date, array $data)
    {
        $time = strtotime($date);
        $channel = 'MQ_TIME_'.$channel;
        $data = $this->_rand_data($channel, $data);
        $this->redis->zAdd($channel, $time, $data);
    }

    /**
     * 防止key重复
     * @param       $channel
     * @param array $data
     *
     * @return string
     */
    private function _rand_data($channel, array $data)
    {
        $key = json_encode($data);
        if(!$this->redis->zScore($channel, $key)){
            return json_encode($data);
        }
        $data['_rand'] = mt_rand(10000, 99999);
        return $this->_rand_data($channel, $data);

    }

    /**
     * 定时队列消息到时间转成实时订阅消息
     * 需要计划任务实时执行, 消息重复一个渠道只能由一个脚本程序执行
     * @param $channel
     */
    function time_to_real($channel)
    {
        $_channel = 'MQ_TIME_'.$channel;
        $time = time();
        $list = $this->redis->zRangeByScore($_channel, 1483200000, $time);
        $this->redis->zRemRangeByScore($_channel, 1483200000, $time);

        if(is_array($list)){
            foreach($list as $item){
                $data = json_decode($item, true);
                $this->real_publish($channel, $data);
            }
        }
    }
}