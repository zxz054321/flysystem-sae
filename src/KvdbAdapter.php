<?php

namespace Coldwind\Filesystem;

use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;

class KvdbAdapter implements AdapterInterface
{

    use NotSupportingVisibilityTrait,
        StreamedTrait;
    protected $client;

    public function __construct(KvdbClient $client)
    {
        $this->client = $client;
    }

    public function copy($path, $newpath)
    {
        $src = $this->client->get($path, false);

        return $this->client->add($newpath, $src, false);
    }

    public function createDir($dirname, Config $config)
    {
        return $this->has($dirname) ?
            false :
            ['path' => $dirname, 'type' => 'dir'];
    }

    public function delete($path)
    {
        return $this->client->delete($path);
    }

    public function deleteDir($dirname)
    {
        return true;
    }

    public function getMetadata($path)
    {
        $metadata = $this->client->get($path) + ['path' => $path];

        unset($metadata['contents']);

        return $metadata;
    }

    public function getMimetype($path)
    {
        $mimetype = Util::guessMimeType($path, $this->read($path)['contents']);

        return [
            'mimetype' => $mimetype,
            'path' => $path,
        ];
    }

    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    public function has($path)
    {
        return $this->client->get($path, false) !== false;
    }

    public function listContents($directory = '', $recursive = false)
    {
        $filter = //禁止递归
            function (&$value, $path) use($directory) {
            if (Util::dirname($path) !== $directory) {
                $value = null;
            }
        };

        $list = $this->client->lists($directory, $recursive ? null : $filter);

        return $list;
    }

    public function read($path)
    {
        $file = $this->client->get($path);

        return [
            'path' => $path,
            'contents' => $file['contents'],
        ];
    }

    public function rename($path, $newpath)
    {
        return $this->copy($path, $newpath) && $this->delete($path);
    }

    public function update($path, $contents, Config $config)
    {
        return $this->kvPut($path, $contents, $config);
    }

    public function write($path, $contents, Config $config)
    {
        $data['type']       = 'file';
        $data['visibility'] = AdapterInterface::VISIBILITY_PUBLIC;

        return $this->kvPut($path, $contents, $config, $data);
    }

    protected function kvPut($path, $contents, Config $config, $file = null)
    {
        $file['contents']  = $contents;
        $file['timestamp'] = time();
        $file['size']      = Util::contentSize($contents);

        if ($visibility = $config->get('visibility')) {
            $file['visibility'] = $visibility;
        }

        $addOrSet = is_null($file) ? 'set' : 'add';

        if ($this->client->$addOrSet($path, $file)) {
            return $file + compact('path');
        }

        return false;
    }
}