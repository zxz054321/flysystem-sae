<?php

namespace Coldwind\Flysystem;

use League\Flysystem\Exception;

class KvdbClient
{
    protected $kvdb;

    public function __construct()
    {
        $this->kvdb = new \SaeKV();

        if (!$this->kvdb->init()) {
            throw new Exception('KVDB无法初始化');
        }
    }

    public function __call($name, $arguments)
    {
        return call_user_func_array(
//            [$this->kvdb, $name], $arguments
        );
    }

    public function get($key, $unserialize = true)
    {
        $data = $this->kvdb->get($key);

        return $unserialize ? unserialize($data) : $data;
    }

    public function set($key, $value)
    {
        return $this->kvdb->set($key, serialize($value));
    }

    public function add($key, $value, $serialize = true)
    {
        $value = $serialize ? serialize($value) : $value;

        return $this->kvdb->add($key, $value);
    }

    public function pkrget($prefix_key, $count = \SaeKV::MAX_PKRGET_SIZE,
                           $start_key = '')
    {
        return $this->kvdb->pkrget($prefix_key, $count, $start_key);
    }

    public function lists($prefix, callable $filter = null,
                          $count = \SaeKV::MAX_PKRGET_SIZE)
    {
        static $_startKey = '';
        static $_data     = array();

        $data = $this->kvdb->pkrget(
            $prefix, $count > 100 ? 100 : $count, $_startKey
        );

        if (is_callable($filter)) {
            if (!array_walk($data, $filter)) {
                return false;
            }

            $data = array_filter($data);
        }

        array_merge($_data, $data);

        //递归遍历
        if ($count >= 100) {
            $count-=100;

            $_startKey = key(end($data));

            $this->lists($count);
        }
        //储存结果
        else {
            $data = $_data;

            unset($_startKey);
            unset($_data);

            return $data;
        }
    }
}