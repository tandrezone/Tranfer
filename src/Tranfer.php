<?php
/**
 * Created by PhpStorm.
 * User: tiagoandre
 * Date: 2019-02-05
 * Time: 14:06
 */

namespace Marvil\Transfer;

use Aws\S3\S3Client;
use Illuminate\Database\Eloquent\Collection;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use App\Demat\Helpers\errorManager;
use App\Demat\Helpers\fileLogManager;
use App\Models\ErrorsModel;
use App\Models\FileLogModel;
use App\Models\LogModel;
use Illuminate\Support\Facades\Storage;
use League\Flysystem\Sftp\SftpAdapter as SftpAdapter;
use League\Flysystem\Adapter\Ftp as FtpAdapter;
use App\Models\SourcesModel;
use League\Flysystem\Filesystem;
use App\Demat\Helpers\sourceFileSystem;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use League\Flysystem\AzureBlobStorage\AzureBlobStorageAdapter as AzureBlobStorageAdapter;

class Transfer
{
    /**
     * @param $type driver or type of connection
     *
     * @return the adpter class
     */
    public static function getAdapter($type){
        switch ($type){
        case 'ftp':
            $adapter = FtpAdapter::class;
            break;
        case 'sftp':
            $adapter = SftpAdapter::class;
            break;
        case 'azbsa':
            $adapter = AzureBlobStorageAdapter::class;
            break;
        case 'aws':
            $adapter = AwsS3Adapter::class;
            break;
        }
        return $adapter;
    }

    /**
     * @param $id the filystem id
     * @param $driver the filesystem driver
     * @param $host the filesystem host
     * @param $username the filesystem username
     * @param $password the filesystem password
     * @param $root the filesystem root
     * @param $timeout the filesystem paassword
     *
     * @return Filesystem
     */
    public static function createSourceFileSystem($id, $driver, $host, $username,$password,$root, $timeout) {
        $sourceFileSystems = array();
        $source = ['id'=>$id, 'driver' => $driver, 'host' => $host, 'username' => $username, 'password' => $password, 'root'=>$root, 'timeout'=>$timeout ];

            $adapter = Transfer::getAdapter($driver);

            $filesystem = new Filesystem(
                new $adapter(
                    array(
                        'source_id' =>$id,
                        'driver' =>$driver,
                        'host' => $host,
                        'username' => $username,
                        'password' => $password,
                        'root' => $root,
                        'timeout' => $timeout,
                    )
                )
            );
        return $filesystem;
    }

    /**
     * @param array $sourceNames
     *
     * @return file paths or Filelog
     */
    public static function createSourceFileSystems(array $sourceNames) {

        $sourceFileSystems = array();

        $sources = SourcesModel::whereIn('name',$sourceNames)->get();
        if($sources->isEmpty()){
            exit("Source with the names ".json_encode($sourceNames). "could not be found in the database, please verify if you have the tables up to date");
        }

        foreach ($sources as $source){
            $adapter = Transfer::getAdapter($source->getAttribute('type'));

            $filesystem = new Filesystem(
                new $adapter(
                    array(
                        'source_id' =>$source->getAttribute('id'),
                        'driver' => $source->getAttribute('type'),
                        'host' => $source->getAttribute('server'),
                        'username' => $source->getAttribute('username'),
                        'password' => $source->getAttribute('password'),
                        'root' => $source->getAttribute('path'),
                        'timeout' => $source->getAttribute('timeout'),
                    )
                )
            );
            $sourceFileSystem = new sourceFileSystem($source->getAttribute('id'),$source->getAttribute('name'),$filesystem);

            $sourceFileSystems[] = $sourceFileSystem;

        }

        return $sourceFileSystems;
    }


    /**
     *
     * @param $sources (if null create sources from the sources table)
     * @param $outFolder path to the folder where the files are stored
     *
     * @return array
     */
    public static function transferFilesFromSources(array $sourceFileSystems = null, array $filetypes = array('csv'), string $outFolder = '/import/', $logs = false)
    {
        $result = array();
        foreach ($sourceFileSystems as $sourceFileSystem) {
            try {
                $files = $sourceFileSystem->filesystem->listContents('',false);

                foreach ($files as $file) {
                    if(isset($file['extension']) ) {
                        if (in_array($file['extension'], $filetypes)) {
                            dump($file);
                            if (!Storage::disk('local')->exists(
                                $outFolder . $sourceFileSystem->name . "/"
                                . $file['basename']
                            )
                            ) {
                                Storage::disk('local')->put(
                                    $outFolder . $sourceFileSystem->name . "/"
                                    . $file['basename'],
                                    $sourceFileSystem->filesystem->read(
                                        $file['path']
                                    )
                                );
                                $result[] = $outFolder . $sourceFileSystem->name
                                    . "/" . $file['basename'];

                                if (getenv("CLEAN_REMOTE_SERVER")) {
                                    $sourceFileSystem->filesystem->delete(
                                        $file['path']
                                    );
                                }
                            } else {
                                $result[] = Storage::disk('local')->url(
                                    $outFolder . $sourceFileSystem->name . "/"
                                    . $file['basename']
                                );

                                if ($logs) {
                                    $errorCheck = new errorManager(
                                        "warning", null, null,
                                        "file already exist"
                                    );
                                }
                            }

                            if ($logs) {
                                $fileLog = new fileLogManager(
                                    $sourceFileSystem->id,
                                    $outFolder . $sourceFileSystem->name . "/",
                                    $file['basename'], "IN", "TRANSFERED"
                                );
                            }

                        } else {
                            if ($logs) {
                                $errorCheck = new errorManager(
                                    "warning", $sourceFileSystem->id, null,
                                    "File " . $file['basename']
                                    . " is not a CSV"
                                );
                            }
                        }
                    }
                }
                if (empty($files)) {
                    if($logs){$errorCheck =new errorManager("warning",$sourceFileSystem->id,null,"No files in source");}

                }

            } catch (\Exception $e) {
                if($logs){$errorCheck =new errorManager("warning",$sourceFileSystem->id,null,$e->getMessage());}
            }
        }

        return $result;
    }


    /**
     * @param array|null $sourceFileSystems
     * @param array      $files
     * @param string     $outFolder
     * @param bool       $logs
     *
     * @return array
     */
    public static function putFilesInSources(array $sourceFileSystems = null, array $files, string $outFolder = '/import/', $logs = false){
        $result = array();
        foreach ($sourceFileSystems as $sourceFileSystem) {
            try {
                foreach ($files as $file) {
                    if (!$sourceFileSystem->filesystem->has($file->path)) {
                        $sourceFileSystem->filesystem->write($file->path,Storage::disk('local')->get($outFolder . $file->path . "/" . $file->basename));
                    } else {
                        if($logs){$errorCheck = new errorManager("warning", $file->source_id, $file->id, "file already exist");}
                    }
                    if($logs){$fileLog =new fileLogManager($sourceFileSystem->id, $outFolder . $sourceFileSystem->name . "/", $file['basename'], "IN", "TRANSFERED");}
                }

            } catch (\Exception $e) {
                if($logs){$errorCheck =new errorManager("warning",$sourceFileSystem->id,null,$e->getMessage());}
            }
        }
        return $result;
    }



}
