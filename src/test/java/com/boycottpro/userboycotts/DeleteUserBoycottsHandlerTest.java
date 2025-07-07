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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
        event.setPathParameters(Map.of("user_id", testUserId, "company_id", testCompanyId));

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
}
