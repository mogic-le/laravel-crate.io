<?php

namespace RatkoR\Crate;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Support\Facades\Config;

class Blob
{
    protected static function getBaseUrl($table)
    {
        $config = Config::get('database.connections.crate');
        return 'http://'.$config['host'] . ':'. $config['port'] . '/_blobs/' . $table . '/';
    }

    /**
     * Uploads given file to Crate.
     *
     * @param  string $content content that will be stored
     * @return string|boolean       Digest hash if store was successfull, false otherwise
     */
    public static function set($content, $table = 'myblobs')
    {
        $hash = sha1($content);
        $url = self::getBaseUrl($table) . $hash;
        $client = new Client();
        $response = null;
        try {
            $client->put($url, ['body' => $content]);
        } catch (ClientException $ce) {
            if ($ce->getCode() == 409) {
                return $hash;
            }
            return false;
        } catch (\Exception $e) {

            return false;
        }

        return $hash;
    }

    /**
     * Downloads file with given digest hash
     *
     * @param string  $hash
     * @param string $table
     *
     * @return string|boolean
     */
    public static function get($hash, $table = 'myblobs')
    {
        $url = self::getBaseUrl($table) . $hash;
        $client = new Client();
        try {
            $response = $client->get($url);
            if ($response->getStatusCode() != 200) {
                throw new \Exception('no content found for hash: '.$hash);
            }
        } catch (\Exception $e) {
            return false;
        }

        return (string)$response->getBody();
    }


    /**
     * check file for given digest hash
     *
     * @param string  $hash
     * @param string $table
     *
     * @return string|boolean
     */
    public static function has($hash, $table = 'myblobs')
    {
        $url = self::getBaseUrl($table) . $hash;
        $client = new Client();
        try {
            $client->head($url);
        } catch (\Exception $e) {
            return false;
        }

        return true;
    }

    /**
     * remove file for given digest hash
     *
     * @param string  $hash
     * @param string $table
     *
     * @return boolean
     */
    public static function remove($hash, $table = 'myblobs')
    {
        $url = self::getBaseUrl($table) . $hash;
        $client = new Client();
        try {
            $client->delete($url);
        } catch (\Exception $e) {
            return false;
        }

        return true;
    }

}
