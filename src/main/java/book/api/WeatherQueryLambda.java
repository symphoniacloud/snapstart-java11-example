package book.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class WeatherQueryLambda {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final String tableName = System.getenv("LOCATIONS_TABLE");

    private static final String DEFAULT_LIMIT = "50";

    public ApiGatewayResponse handler(ApiGatewayRequest request) throws IOException {
        System.out.println("** 1 **");
        final String limitParam = request.queryStringParameters == null
                ? DEFAULT_LIMIT
                : request.queryStringParameters.getOrDefault("limit", DEFAULT_LIMIT);
        System.out.println("** 2 **");
        final int limit = Integer.parseInt(limitParam);

        System.out.println("** 3 **");
        final ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName)
                .withLimit(limit);
        System.out.println("** 4 **");
        final ScanResult scanResult = dynamoDB.scan(scanRequest);

        System.out.println("** 5 **");
        final List<WeatherEvent> events = scanResult.getItems().stream()
                .map(item -> new WeatherEvent(
                        item.get("locationName").getS(),
                        Double.parseDouble(item.get("temperature").getN()),
                        Long.parseLong(item.get("timestamp").getN()),
                        Double.parseDouble(item.get("longitude").getN()),
                        Double.parseDouble(item.get("latitude").getN())
                ))
                .collect(Collectors.toList());

        System.out.println("** 6 **");
        final String json = objectMapper.writeValueAsString(events);

        System.out.println("** 7 **");
        return new ApiGatewayResponse(200, json);
    }
}
