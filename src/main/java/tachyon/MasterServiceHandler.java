package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

import CodeTracer.CT;
import org.apache.commons.lang.StringUtils;

/**
 * The Master server program.
 * 
 * It maintains the state of each worker. It never keeps the state of any user.
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final MasterInfo mMasterInfo;

  public MasterServiceHandler(MasterInfo masterInfo) {
		try (CT _ = new CT(masterInfo)) {
    mMasterInfo = masterInfo;
  } }

  @Override
  public boolean addCheckpoint(long workerId, int fileId, long fileSizeBytes, String checkpointPath) 
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
		try (CT _ = new CT(workerId, fileId, fileSizeBytes, checkpointPath)) {
    boolean b = mMasterInfo.addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
    _.Returns(b);
    return b;
  } }

  @Override
  public List<ClientFileInfo> cmd_ls(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
		try (CT _ = new CT(path)) {
    return mMasterInfo.getFilesInfo(path);
  } }

  @Override
  public int user_createFile(String filePath)
      throws FileAlreadyExistException, InvalidPathException, TException {
		try (CT _ = new CT(filePath)) {
    int cf = mMasterInfo.createFile(filePath, false);
    _.Returns(cf);
    return cf;
  } }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException, TException {
		try (CT _ = new CT(path, columns, metadata)) {
    return mMasterInfo.createRawTable(
        path, columns, CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  } }

  @Override
  public void user_deleteById(int id) throws TException {
		try (CT _ = new CT(id)) {
    mMasterInfo.delete(id);
  } }

  @Override
  public void user_deleteByPath(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
		try (CT _ = new CT(path)) {
    mMasterInfo.delete(path);
  } }

  @Override
  public NetAddress user_getWorker(boolean random, String host) 
      throws NoLocalWorkerException, TException {
		try (CT _ = new CT(random, host)) {
    NetAddress na = mMasterInfo.getWorker(random, host);
    _.Returns(na);
    return na;
  } }

  @Override
  public ClientFileInfo user_getClientFileInfoById(int id)
      throws FileDoesNotExistException, TException {
		try (CT _ = new CT(id)) {
      ClientFileInfo cfi = mMasterInfo.getClientFileInfo(id);
      _.Returns(cfi);
      return cfi;
  } }

  @Override
  public ClientFileInfo user_getClientFileInfoByPath(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT(path)) {
    return mMasterInfo.getClientFileInfo(path);
  } }

  @Override
  public List<NetAddress> user_getFileLocationsById(int fileId)
      throws FileDoesNotExistException, TException {
		try (CT _ = new CT(fileId)) {
    List<NetAddress> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(fileId);
      //_.Returns(StringUtils.join(ret, ", "));
      _.Returns(ret);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  } }

  @Override
  public List<NetAddress> user_getFileLocationsByPath(String filePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT(filePath)) {
    List<NetAddress> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(filePath);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  } }

  @Override
  public int user_getFileId(String filePath) throws InvalidPathException, TException {
		try (CT _ = new CT(filePath)) {
    int fid = mMasterInfo.getFileId(filePath);
    _.Returns(fid);
    return fid;
  } }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
		try (CT _ = new CT()) {
    return mMasterInfo.getRawTableId(path);
  } }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws TableDoesNotExistException, TException {
		try (CT _ = new CT()) {
    return mMasterInfo.getClientRawTableInfo(id);
  } }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT()) {
    return mMasterInfo.getClientRawTableInfo(path);
  } }

  @Override
  public long user_getUserId() throws TException {
		try (CT _ = new CT()) {
    long new_uid = mMasterInfo.getNewUserId();
    _.Returns(new_uid);
    return new_uid;
  } }

  @Override
  public int user_getNumberOfFiles(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT()) {
    return mMasterInfo.getNumberOfFiles(path);
  } }

  @Override
  public String user_getUnderfsAddress() throws TException {
		try (CT _ = new CT()) {
    String ufs_addr = CommonConf.get().UNDERFS_ADDRESS;
    _.Returns(ufs_addr);
    return ufs_addr;
  } }

  @Override
  public List<Integer> user_listFiles(String path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT(path, recursive)) {
    return mMasterInfo.listFiles(path, recursive);
  } }

  @Override
  public List<String> user_ls(String path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, TException {
		try (CT _ = new CT(path, recursive)) {
    return mMasterInfo.ls(path, recursive);
  } }

  @Override
  public int user_mkdir(String path) 
      throws FileAlreadyExistException, InvalidPathException, TException {
		try (CT _ = new CT(path)) {
    return mMasterInfo.createFile(path, true);
  } }

  @Override
  public void user_outOfMemoryForPinFile(int fileId) throws TException {
		try (CT _ = new CT(fileId)) {
    LOG.error("The user can not allocate enough space for PIN list File " + fileId);
  } }

  @Override
  public void user_renameFile(String srcFilePath, String dstFilePath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException, TException{
		try (CT _ = new CT(srcFilePath, dstFilePath)) {
    mMasterInfo.renameFile(srcFilePath, dstFilePath);
  } }

  @Override
  public void user_unpinFile(int fileId) throws FileDoesNotExistException, TException {
		try (CT _ = new CT(fileId)) {
    mMasterInfo.unpinFile(fileId);
  } }

  @Override
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TException {
		try (CT _ = new CT()) {
    mMasterInfo.updateRawTableMetadata(tableId, 
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  } }

  @Override
  public void worker_cacheFile(long workerId, long workerUsedBytes, int fileId,
      long fileSizeBytes) throws FileDoesNotExistException,
      SuspectedFileSizeException, TException {
		try (CT _ = new CT(workerId, workerUsedBytes, fileId, fileSizeBytes)) {
    mMasterInfo.cachedFile(workerId, workerUsedBytes, fileId, fileSizeBytes);
  } }

  @Override
  public Set<Integer> worker_getPinIdList() throws TException {
    List<Integer> ret = mMasterInfo.getPinIdList();
		try (CT _ = new CT()) {
    return new HashSet<Integer>(ret);
  } }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Integer> removedFileIds)
      throws TException {
		//try (CT _ = new CT()) {
    return mMasterInfo.workerHeartbeat(workerId, usedBytes, removedFileIds);
  } //}

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Integer> currentFileIds) throws TException {
		try (CT _ = new CT(workerNetAddress, totalBytes, usedBytes, currentFileIds)) {
    return mMasterInfo.registerWorker(workerNetAddress, totalBytes, usedBytes, currentFileIds);
  } }
}
