package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.boycottpro.models.ResponseMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.lang.reflect.Field;
import com.fasterxml.jackson.core.JsonProcessingException;

@ExtendWith(MockitoExtension.class)
class DeleteUserBoycottsHandlerTest {

    @Mock
    private DynamoDbClient dynamoDb;

    @InjectMocks
    private DeleteUserBoycottsHandler handler;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSuccessfulDeleteFlow() throws Exception {
        String testUserId = "user-123";
        String testCompanyId = "company-456";

        // Mock query response for user_boycotts
        Map<String, AttributeValue> mockItem = Map.of(
                "user_id", AttributeValue.fromS(testUserId),
                "company_id", AttributeValue.fromS(testCompanyId),
                "company_name", AttributeValue.fromS("Test Company"),
                "company_cause_id", AttributeValue.fromS("com#cause"),
                "cause_id", AttributeValue.fromS("cause")
        );
        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);

        // Mock batch write response
        BatchWriteItemResponse mockBatchResponse = BatchWriteItemResponse.builder().build();
        when(dynamoDb.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(mockBatchResponse);

        // Mock update item response
        UpdateItemResponse mockUpdateResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(mockUpdateResponse);

        // Build request
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of( "company_id", testCompanyId));

        // Invoke handler
        APIGatewayProxyResponseEvent response = handler.handleRequest(event, mock(Context.class));

        assertEquals(200, response.getStatusCode());
        ResponseMessage message = objectMapper.readValue(response.getBody(), ResponseMessage.class);
        assertTrue(message.getMessage().contains("boycott removed successfully"));

        // Verify interactions
        verify(dynamoDb, times(1)).query(any(QueryRequest.class));
        verify(dynamoDb, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(dynamoDb, times(2)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void testDefaultConstructor() {
        // Test the default constructor coverage
        // Note: This may fail in environments without AWS credentials/region configured
        try {
            DeleteUserBoycottsHandler handler = new DeleteUserBoycottsHandler();
            assertNotNull(handler);

            // Verify DynamoDbClient was created (using reflection to access private field)
            try {
                Field dynamoDbField = DeleteUserBoycottsHandler.class.getDeclaredField("dynamoDb");
                dynamoDbField.setAccessible(true);
                DynamoDbClient dynamoDb = (DynamoDbClient) dynamoDbField.get(handler);
                assertNotNull(dynamoDb);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                fail("Failed to access DynamoDbClient field: " + e.getMessage());
            }
        } catch (software.amazon.awssdk.core.exception.SdkClientException e) {
            // AWS SDK can't initialize due to missing region configuration
            // This is expected in Jenkins without AWS credentials - test passes
            System.out.println("Skipping DynamoDbClient verification due to AWS SDK configuration: " + e.getMessage());
        }
    }

    @Test
    public void testUnauthorizedUser() {
        // Test the unauthorized block coverage
        handler = new DeleteUserBoycottsHandler(dynamoDb);

        // Create event without JWT token (or invalid token that returns null sub)
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        // No authorizer context, so JwtUtility.getSubFromRestEvent will return null

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(401, response.getStatusCode());
        assertTrue(response.getBody().contains("Unauthorized"));
    }

    @Test
    public void testJsonProcessingExceptionInResponse() throws Exception {
        // Test JsonProcessingException coverage in response method by using reflection
        handler = new DeleteUserBoycottsHandler(dynamoDb);

        // Use reflection to access the private response method
        java.lang.reflect.Method responseMethod = DeleteUserBoycottsHandler.class.getDeclaredMethod("response", int.class, Object.class);
        responseMethod.setAccessible(true);

        // Create an object that will cause JsonProcessingException
        Object problematicObject = new Object() {
            public Object writeReplace() throws java.io.ObjectStreamException {
                throw new java.io.NotSerializableException("Not serializable");
            }
        };

        // Create a circular reference object that will cause JsonProcessingException
        Map<String, Object> circularMap = new HashMap<>();
        circularMap.put("self", circularMap);

        // This should trigger the JsonProcessingException -> RuntimeException path
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            try {
                responseMethod.invoke(handler, 500, circularMap);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                throw new RuntimeException(e.getCause());
            }
        });

        // Verify it's ultimately caused by JsonProcessingException
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof JsonProcessingException,
                "Expected JsonProcessingException, got: " + cause.getClass().getSimpleName());
    }

    @Test
    public void testMissingCompanyId() throws Exception {
        // Test lines 47-48, 51: when company_id is null
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        // No path parameters set

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(400, response.getStatusCode());
        assertTrue(response.getBody().contains("company_id not present"));
    }

    @Test
    public void testEmptyCompanyId() throws Exception {
        // Test lines 46: when company_id is empty string
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", ""));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(400, response.getStatusCode());
        assertTrue(response.getBody().contains("company_id not present"));
    }

    @Test
    public void testEmptyCauseIdsList() throws Exception {
        // Test line 56: when causeIds list is empty (no matching records)
        String testCompanyId = "company-456";

        // Mock query response with no items
        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of())
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(200, response.getStatusCode());
        // Verify decrement methods were NOT called
        verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void testExceptionHandling() throws Exception {
        // Test lines 67-69 and 136-138: exception in deleteUserBoycotts is caught
        // The exception in deleteUserBoycotts returns empty list, so handler succeeds
        String testCompanyId = "company-456";

        // Mock query to throw exception
        when(dynamoDb.query(any(QueryRequest.class))).thenThrow(new RuntimeException("DynamoDB error"));

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        // Returns 200 because deleteUserBoycotts catches exception and returns empty list (lines 136-138)
        assertEquals(200, response.getStatusCode());
        assertTrue(response.getBody().contains("boycott removed successfully"));
    }

    @Test
    public void testItemWithoutCompanyIdKey() throws Exception {
        // Test lines 99-100: item without company_id key
        String testCompanyId = "company-456";

        // Mock item without company_id key
        Map<String, AttributeValue> mockItem = Map.of(
                "user_id", AttributeValue.fromS("user-123"),
                "company_cause_id", AttributeValue.fromS("com#cause")
        );
        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(200, response.getStatusCode());
        // Should not call batch write since no matching items
        verify(dynamoDb, never()).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testItemWithDifferentCompanyId() throws Exception {
        // Test line 100: item with different company_id
        String testCompanyId = "company-456";

        // Mock item with different company_id
        Map<String, AttributeValue> mockItem = Map.of(
                "user_id", AttributeValue.fromS("user-123"),
                "company_id", AttributeValue.fromS("different-company"),
                "company_cause_id", AttributeValue.fromS("com#cause")
        );
        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(200, response.getStatusCode());
        // Should not call batch write since no matching items
        verify(dynamoDb, never()).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testItemWithNullCauseId() throws Exception {
        // Test line 102: item with null cause_id
        String testCompanyId = "company-456";

        // Mock item with null cause_id
        Map<String, AttributeValue> mockItem = new HashMap<>();
        mockItem.put("user_id", AttributeValue.fromS("user-123"));
        mockItem.put("company_id", AttributeValue.fromS(testCompanyId));
        mockItem.put("company_cause_id", AttributeValue.fromS("com#cause"));
        // cause_id is null (not in map)

        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);
        BatchWriteItemResponse mockBatchResponse = BatchWriteItemResponse.builder().build();
        when(dynamoDb.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(mockBatchResponse);
        // No updateItem stub needed - cause_id is null so causeIds list is empty, decrementCauseCompanyStatsRecords not called

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(200, response.getStatusCode());
        // Should still delete the item but not decrement cause stats
        verify(dynamoDb, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testItemWithCompanyIdButNoCompanyCauseId() throws Exception {
        // Test lines 112-113: item with company_id but not company_cause_id
        String testCompanyId = "company-456";

        // Mock item with company_id but no company_cause_id
        Map<String, AttributeValue> mockItem = Map.of(
                "user_id", AttributeValue.fromS("user-123"),
                "company_id", AttributeValue.fromS(testCompanyId),
                "cause_id", AttributeValue.fromS("cause-789")
        );

        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);
        BatchWriteItemResponse mockBatchResponse = BatchWriteItemResponse.builder().build();
        when(dynamoDb.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(mockBatchResponse);
        UpdateItemResponse mockUpdateResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(mockUpdateResponse);

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(200, response.getStatusCode());
        verify(dynamoDb, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testExceptionInDecrementCauseCompanyStatsRecords() throws Exception {
        // Test lines 178-181: exception in decrementCauseCompanyStatsRecords
        String testCompanyId = "company-456";

        Map<String, AttributeValue> mockItem = Map.of(
                "user_id", AttributeValue.fromS("user-123"),
                "company_id", AttributeValue.fromS(testCompanyId),
                "company_cause_id", AttributeValue.fromS("com#cause"),
                "cause_id", AttributeValue.fromS("cause-789")
        );

        QueryResponse mockQueryResponse = QueryResponse.builder()
                .items(List.of(mockItem))
                .build();

        when(dynamoDb.query(any(QueryRequest.class))).thenReturn(mockQueryResponse);
        BatchWriteItemResponse mockBatchResponse = BatchWriteItemResponse.builder().build();
        when(dynamoDb.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(mockBatchResponse);

        // Mock first updateItem to succeed (decrementCompanyBoycottCount)
        // Mock second updateItem to throw exception (decrementCauseCompanyStatsRecords)
        when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build())
                .thenThrow(new RuntimeException("Failed to update cause stats"));

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        event.setPathParameters(Map.of("company_id", testCompanyId));

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        // Should still return 200 because exception is caught in decrementCauseCompanyStatsRecords
        assertEquals(200, response.getStatusCode());
        verify(dynamoDb, times(2)).updateItem(any(UpdateItemRequest.class));
    }

}
