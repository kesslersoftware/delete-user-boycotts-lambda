package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.ResponseMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.stream.Collectors;

public class DeleteUserBoycottsHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String TABLE_NAME = "";
    private final DynamoDbClient dynamoDb;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeleteUserBoycottsHandler() {
        this.dynamoDb = DynamoDbClient.create();
    }

    public DeleteUserBoycottsHandler(DynamoDbClient dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        try {
            Map<String, String> pathParams = event.getPathParameters();
            String userId = (pathParams != null) ? pathParams.get("user_id") : null;
            String companyId = (pathParams != null) ? pathParams.get("company_id") : null;
            if (userId == null || userId.isEmpty()) {
                ResponseMessage message = new ResponseMessage(400,
                        "sorry, there was an error processing your request",
                        "user_id not present");
                String responseBody = objectMapper.writeValueAsString(message);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(400)
                        .withHeaders(Map.of("Content-Type", "application/json"))
                        .withBody(responseBody);
            }
            if (companyId == null || companyId.isEmpty()) {
                ResponseMessage message = new ResponseMessage(400,
                        "sorry, there was an error processing your request",
                        "company_id not present");
                String responseBody = objectMapper.writeValueAsString(message);
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(400)
                        .withHeaders(Map.of("Content-Type", "application/json"))
                        .withBody(responseBody);
            }
            List<String> causeIds = deleteUserBoycotts(userId, companyId);
            if (causeIds.size()>0) {
                decrementCompanyBoycottCount(companyId);
                decrementCauseCompanyStatsRecords(companyId,causeIds);
            }
            ResponseMessage message = new ResponseMessage(200,
                    "boycott removed successfully",
                    "user_boycotts record deleted along with all records from other tables");
            String responseBody = objectMapper.writeValueAsString(message);
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(200)
                    .withHeaders(Map.of("Content-Type", "application/json"))
                    .withBody(responseBody);
        } catch (Exception e) {
            e.printStackTrace();
            ResponseMessage message = new ResponseMessage(500,
                    "sorry, there was an error processing your request",
                    "Unexpected server error: " + e.getMessage());
            String responseBody = null;
            try {
                responseBody = objectMapper.writeValueAsString(message);
            } catch (JsonProcessingException ex) {
                System.out.println("json processing exception");
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withHeaders(Map.of("Content-Type", "application/json"))
                    .withBody(responseBody);
        }
    }
    private List<String> deleteUserBoycotts(String userId, String companyId) {
        try {
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName("user_boycotts")
                    .keyConditionExpression("user_id = :uid")
                    .expressionAttributeValues(Map.of(
                            ":uid", AttributeValue.fromS(userId)
                    ))
                    .build();

            QueryResponse queryResponse = dynamoDb.query(queryRequest);

            List<WriteRequest> deleteRequests = new ArrayList<>();
            List<String> causeIds = new ArrayList<>();
            for (Map<String, AttributeValue> item : queryResponse.items()) {
                if (!item.containsKey("company_id")) continue;
                if (!item.get("company_id").s().equals(companyId)) continue;
                String causeId = null;
                if(item.get("cause_id") != null) {
                    causeId = item.get("cause_id").s();
                    causeIds.add(causeId);
                }
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("user_id", item.get("user_id"));

                // Handle key schema: if your sort key is a composite (e.g. company_id#cause_id), adjust here
                if (item.containsKey("company_cause_id")) {
                    key.put("company_cause_id", item.get("company_cause_id"));
                } else if (item.containsKey("company_id")) {
                    key.put("company_id", item.get("company_id"));
                }

                deleteRequests.add(WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder().key(key).build())
                        .build());
            }

            if (deleteRequests.isEmpty()) {
                return new ArrayList();
            }

            // Batch delete (25 max per call)
            for (int i = 0; i < deleteRequests.size(); i += 25) {
                List<WriteRequest> batch = deleteRequests.subList(i, Math.min(i + 25, deleteRequests.size()));
                BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                        .requestItems(Map.of("user_boycotts", batch))
                        .build();
                dynamoDb.batchWriteItem(batchRequest);
            }

            return causeIds;

        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList();
        }
    }
    private void decrementCompanyBoycottCount(String companyId) {
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName("companies")
                .key(Map.of("company_id", AttributeValue.fromS(companyId)))
                .updateExpression("SET boycott_count = boycott_count - :inc")
                .expressionAttributeValues(Map.of(":inc", AttributeValue.fromN("1")))
                .conditionExpression("attribute_exists(company_id) AND boycott_count > :zero")
                .expressionAttributeValues(Map.ofEntries(
                        Map.entry(":inc", AttributeValue.fromN("1")),
                        Map.entry(":zero", AttributeValue.fromN("0"))
                ))
                .build();

        dynamoDb.updateItem(updateRequest);
    }
    public void decrementCauseCompanyStatsRecords(String companyId, List<String> causeIds) {
        for (String causeId : causeIds) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("company_id", AttributeValue.fromS(companyId));
                key.put("cause_id", AttributeValue.fromS(causeId));

                // Use ADD to decrement the counter atomically
                Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
                attributeUpdates.put("boycott_count", AttributeValueUpdate.builder()
                        .value(AttributeValue.fromN("-1"))
                        .action(AttributeAction.ADD)
                        .build());

                UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                        .tableName("cause_company_stats")
                        .key(key)
                        .attributeUpdates(attributeUpdates)
                        .build();

                try {
                    dynamoDb.updateItem(updateRequest);
                } catch (Exception e) {
                    System.err.println("Failed to decrement boycott_count for company_id=" + companyId +
                            ", cause_id=" + causeId);
                    e.printStackTrace();
                }
        }
    }


}