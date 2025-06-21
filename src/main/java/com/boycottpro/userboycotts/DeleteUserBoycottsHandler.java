package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

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
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(400)
                        .withBody("{\"error\":\"Missing user_id in path\"}");
            }
            if (companyId == null || companyId.isEmpty()) {
                return new APIGatewayProxyResponseEvent()
                        .withStatusCode(400)
                        .withBody("{\"error\":\"Missing company_id in path\"}");
            }
            boolean deleted = deleteUserBoycotts(userId, companyId);
            if (deleted) {
                decrementCompanyBoycottCount(companyId);
            }
            String responseBody = objectMapper.writeValueAsString("user_boycotts record delete = " + deleted);
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(200)
                    .withHeaders(Map.of("Content-Type", "application/json"))
                    .withBody(responseBody);
        } catch (Exception e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("{\"error\": \"Unexpected server error: " + e.getMessage() + "\"}");
        }
    }
    private boolean deleteUserBoycotts(String userId, String companyId) {
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

            for (Map<String, AttributeValue> item : queryResponse.items()) {
                if (!item.containsKey("company_id")) continue;
                if (!item.get("company_id").s().equals(companyId)) continue;

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
                return true;
            }

            // Batch delete (25 max per call)
            for (int i = 0; i < deleteRequests.size(); i += 25) {
                List<WriteRequest> batch = deleteRequests.subList(i, Math.min(i + 25, deleteRequests.size()));
                BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                        .requestItems(Map.of("user_boycotts", batch))
                        .build();
                dynamoDb.batchWriteItem(batchRequest);
            }

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
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

}