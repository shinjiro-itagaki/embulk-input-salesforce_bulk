package org.embulk.input.salesforce_bulk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ContentType;
import com.sforce.async.CSVReader;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * SalesforceBulkWrapper.
 *
 * -- example:
 * <pre>
 * {@code
 * SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
 *         USER_NAME,
 *         PASSWORD,
 *         AUTH_ENDPOINT_URL,
 *         IS_COMPRESSION,
 *         POLLING_INTERVAL_MILLISECOND);
 * int results = sfbw.syncQuery(
 *         "Account", "SELECT Id, Name FROM Account ORDER BY Id", new SalesforceBulkWrapper.Each<String,String>() { public Map<String,String> each(Map<String,String> row){ ... }});
 * sfbw.close();
 * }
 * </pre>
 */
public class SalesforceBulkWrapper implements AutoCloseable {

    // コネクション
    private PartnerConnection partnerConnection;
    private BulkConnection bulkConnection;

    // Bulk 接続設定
    private boolean isCompression;
    private int pollingIntervalMillisecond;
    private boolean queryAll;

    private static final String API_VERSION = "39.0";
    private static final String AUTH_ENDPOINT_URL_DEFAULT =
            "https://login.salesforce.com/services/Soap/u/" + API_VERSION;

    private static final boolean IS_COMPRESSION_DEFAULT = true;
    private static final int POLLING_INTERVAL_MILLISECOND_DEFAULT = 30000;
    private static final boolean QUERY_ALL_DEFAULT = false;

    public interface Each<K,V>{
        public Map<K,V> each(Map<K,V> record);
    }
    
    /**
     * Constructor
     */
    public SalesforceBulkWrapper(String userName, String password)
            throws AsyncApiException, ConnectionException {
        this(userName,
                password,
                AUTH_ENDPOINT_URL_DEFAULT,
                IS_COMPRESSION_DEFAULT,
                POLLING_INTERVAL_MILLISECOND_DEFAULT,
                QUERY_ALL_DEFAULT);
    }

    /**
     * Constructor
     */
    public SalesforceBulkWrapper(
            String userName,
            String password,
            String authEndpointUrl,
            boolean isCompression,
            int pollingIntervalMillisecond,
            boolean queryAll)
            throws AsyncApiException, ConnectionException {

        partnerConnection = createPartnerConnection(
                authEndpointUrl,
                userName,
                password);
        bulkConnection = createBulkConnection(partnerConnection.getConfig());

        this.pollingIntervalMillisecond = pollingIntervalMillisecond;
        this.queryAll = queryAll;
    }

    public int syncQuery(String objectType, String query, Each<String,String> hook)
        throws InterruptedException, AsyncApiException, IOException {

        // ジョブ作成
        JobInfo jobInfo = new JobInfo();
        jobInfo.setObject(objectType);
        if (queryAll) {
            jobInfo.setOperation(OperationEnum.queryAll);
        } else {
            jobInfo.setOperation(OperationEnum.query);
        }
        jobInfo.setContentType(ContentType.CSV);
        jobInfo = bulkConnection.createJob(jobInfo);

        // バッチ作成
        InputStream is = new ByteArrayInputStream(query.getBytes());
        BatchInfo batchInfo = bulkConnection.createBatchFromStream(jobInfo, is);

        // ジョブクローズ
        JobInfo closeJob = new JobInfo();
        closeJob.setId(jobInfo.getId());
        closeJob.setState(JobStateEnum.Closed);
        bulkConnection.updateJob(closeJob);

        // 実行状況取得
        batchInfo = waitBatch(batchInfo);
        BatchStateEnum state = batchInfo.getState();

        // 実行結果取得
        if (state == BatchStateEnum.Completed) {
            QueryResultList queryResultList =
                    bulkConnection.getQueryResultList(
                            batchInfo.getJobId(),
                            batchInfo.getId());
            return getQueryResultCount(batchInfo, queryResultList, hook);
        } else {
            throw new AsyncApiException(batchInfo.getStateMessage(), AsyncExceptionCode.InvalidBatch);
        }
    }

    public int queryBySoap(String objectType, String query, List<String> select_xs, Each<String,String> func)
        throws InterruptedException, AsyncApiException, IOException, ConnectionException {
        QueryResult results = this.partnerConnection.query(query);
        boolean done = false;
        int rtn = -1;
        if (results.getSize() > 0) {
            rtn = 0;
            while (!done) {
                for (SObject so: results.getRecords()) {
                    Map<String, String> rec = new HashMap<String,String>();
                    for (String name: select_xs){
                        Object v = so.getField(name);
                        rec.put(name, v == null ? null : (String) v);
                    }
                    func.each(rec);
                    rtn += 1;
                }
                if (results.isDone()) {
                    done = true;
                } else {
                    results = this.partnerConnection.queryMore(results.getQueryLocator());
                }
            }
        }
        return rtn;
    }
    
    private int getQueryResultCount(BatchInfo batchInfo,
                                    QueryResultList queryResultList,
                                    Each<String,String> hook)
        throws AsyncApiException, IOException
    {

        int count = 0;

        for (String queryResultId : queryResultList.getResult()) {
            CSVReader rdr =
                new CSVReader(bulkConnection.getQueryResultStream(
                            batchInfo.getJobId(),
                            batchInfo.getId(),
                            queryResultId));

            // バッチ作成時の CSV 制限は今回関係ないのですべて Integer.MAX_VALUE に設定。
            rdr.setMaxRowsInFile(Integer.MAX_VALUE);
            rdr.setMaxCharsInFile(Integer.MAX_VALUE);

            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                HashMap<String, String> rowMap = new HashMap<>(resultCols);
                for (int i = 0; i < resultCols; i++) {
                    rowMap.put(resultHeader.get(i), row.get(i));
                }
                hook.each(rowMap);
                count += 1;
            }
        }
        return count;
    }

    public void close() throws ConnectionException {
        partnerConnection.logout();
    }

    public DescribeGlobalSObjectResult[] getDescribeGlobalSObjectResults() {
        try {
            DescribeGlobalResult describeGlobalResult = this.partnerConnection.describeGlobal();
            if(describeGlobalResult == null) {
                return new DescribeGlobalSObjectResult[0];
            }
            return describeGlobalResult.getSobjects();
        }catch (ConnectionException ce) {
            ce.printStackTrace();
        }
        return new DescribeGlobalSObjectResult[0];
    }

    public Field[] getFieldsOf(String objectName){
        try {
            DescribeSObjectResult[] describeSObjectResults = this.partnerConnection.describeSObjects(new String[]{objectName});
            if (describeSObjectResults != null && describeSObjectResults.length > 0) {
                DescribeSObjectResult describeSObjectResult = describeSObjectResults[0];
                System.out.println("sObject name: " + describeSObjectResult.getName());
                if (describeSObjectResult.isCreateable()){
                    System.out.println("Createable");
                }
                // Get the fields
                return describeSObjectResult.getFields();
            }
        }catch (ConnectionException ce) {
            ce.printStackTrace();
        }
        return new Field[0];
    }
    
    private PartnerConnection createPartnerConnection(
            String endpointUrl,
            String userName,
            String password)
            throws ConnectionException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(endpointUrl);

        return new PartnerConnection(partnerConfig);
    }

    private BulkConnection createBulkConnection(ConnectorConfig partnerConfig)
            throws AsyncApiException {

        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());

        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(
                0, soapEndpoint.indexOf("Soap/")) + "async/" + API_VERSION;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(isCompression);

        config.setTraceMessage(false);

        return new BulkConnection(config);
    }

    private BatchInfo waitBatch(BatchInfo batchInfo)
            throws InterruptedException, AsyncApiException {
        while(true) {
            Thread.sleep(pollingIntervalMillisecond);
            batchInfo = bulkConnection.getBatchInfo(
                    batchInfo.getJobId(),
                    batchInfo.getId());
            BatchStateEnum state = batchInfo.getState();
            if (state == BatchStateEnum.Completed ||
                    state == BatchStateEnum.Failed ||
                    state == BatchStateEnum.NotProcessed) {
                return batchInfo;
            }
        }
    }
}
