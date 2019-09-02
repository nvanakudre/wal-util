package utility.kafka.consumer.db;

import static java.sql.DriverManager.getConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONObject;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Component
public class ConsumerDBOperation {

	private static Logger log = LoggerFactory.getLogger(ConsumerDBOperation.class);

	@Value("${audit.datasource.url}")
	private String dbURL;

	@Value("${audit.datasource.username}")
	private String dbUsername;

	@Value("${audit.datasource.password}")
	private String dbPasswd;

	public static List<String> availableTables = new ArrayList<>();

	public void createTable(String tableName, Map<String, String> columnMap) {
		if (StringUtils.hasText(tableName) && !CollectionUtils.isEmpty(columnMap)) {
			String createTableQuery = "";
			StringBuilder tableQueryBuilder = new StringBuilder("create table if not exists " + tableName + "_log"
					+ "( \"ID\" bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),");
			
			for (Map.Entry<String, String> map : columnMap.entrySet()) {
				// log.info("{},{}", map.getKey(), map.getValue());
				tableQueryBuilder.append(map.getKey().toUpperCase() + " " + map.getValue().toUpperCase() + ",");
			}
			createTableQuery = tableQueryBuilder.toString();
			if (createTableQuery.endsWith(",")) {
				createTableQuery = createTableQuery.substring(0, createTableQuery.length() - 1);
			}
			createTableQuery = createTableQuery + ", CONSTRAINT " + tableName + "_pkey PRIMARY KEY (\"ID\"));";
			log.info("Table insert query: {}", createTableQuery);

			try (Connection logConnection = getConnection(dbURL, getProperties())) {
				try (Statement preparedStatement = logConnection.createStatement()) {
					preparedStatement.executeUpdate(createTableQuery);
					availableTables.add(tableName + "_log");
				}
			} catch (SQLException e) {
				log.info("SQL Exception: {}", e.getMessage());
			}
		}
	}

	/**
	 * Insert WAL log for ddl operations.
	 *
	 * @param data data obtained from WAL
	 * 
	 * @throws SQLException
	 */
	public synchronized void insertWALRecord(String data) throws SQLException, IOException {
		System.out.println("Executing Thread ... "+ Thread.currentThread().getName());
		JSONObject dataObject = new JSONObject(data);
		JSONArray dataArray = dataObject.getJSONArray("change");
		Properties props = getProperties();
		int dataLength = dataArray.length();
		try (Connection logConnection = getConnection(dbURL, props)) {
			try (Statement preparedStatement = logConnection.createStatement()) {
				for (int index = 0; index < dataLength; index++) {
					String operation = (String) dataArray.getJSONObject(index).get("kind"); // operation
					String table = (String) dataArray.getJSONObject(index).get("table");
					if (!table.endsWith("_log") && OPERATION.valueOf(operation).equals(OPERATION.insert)
							|| OPERATION.valueOf(operation).equals(OPERATION.update)) {
						log.info("************Adding chargeback log**************");
						log.info("Perform log for table:{} and for operation:{}", table, operation);

						String query = parseWALLog(dataArray.getJSONObject(index), table, operation);
						preparedStatement.addBatch(query);
					}
				}
				preparedStatement.executeBatch();
			}
		}
		log.info("batch executed");

//		try (Connection logConnection = getConnection(dbURL, props)) {
//			try (Statement preparedStatement = logConnection.createStatement()) {
//				dataArray.forEach(obj -> {
//					JSONObject jsonObject = (JSONObject)obj;
//					String operation = String.valueOf(jsonObject.get("kind"));
//					String table = String.valueOf(jsonObject.get("table"));
//					if (!table.endsWith("_log") && OPERATION.valueOf(operation).equals(OPERATION.insert)
//							|| OPERATION.valueOf(operation).equals(OPERATION.update)) {
//						log.info("************Adding chargeback log**************");
//						log.info("Perform log for table:{} and for operation:{}", table, operation);
//						String query = parseWALLog(jsonObject, table, operation);
//						try {
//							preparedStatement.addBatch(query);
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					}
//				});
//				preparedStatement.executeBatch();
//			}
//		}
		
		
		
	}

	private String parseWALLog(JSONObject object, String table, String operation) {
		JSONArray columnTypes = object.getJSONArray("columntypes");
		JSONArray columnNames = object.getJSONArray("columnnames");
		JSONArray columnValues = object.getJSONArray("columnvalues");
		List<Object> colNames = columnNames.toList();
		List<Object> cnames = colNames.stream().map(s -> "\"" + s + "\"").collect(Collectors.toList());
		cnames.add("\"AUDIT_ADD_DATE\"");
		cnames.add("\"AUDIT_PERFORMED_OPERATION\"");
		cnames.add("\"AUDIT_ADD_USERNAME\"");
		String columns = cnames.toString().replace("[", "(").replace("]", ")");
		List<Object> clist = columnValues.toList();
		String values = "";
		Map<String, String> columnMap = new HashMap<>();
		for (int j = 0; j < columnNames.length(); j++) {
			String type = (String) columnTypes.get(j);
			String val = String.valueOf(clist.get(j));
			columnMap.put(columnNames.get(j).toString(), type);
			switch (type) {
			case "bigint":
				values = StringUtils.hasText(values) ? values.concat("," + val) : values.concat(val);
				break;
			case "name":
				values = StringUtils.hasText(values) ? values.concat(",\'" + val + "\'")
						: values.concat("\'" + val + "\'");
				break;
			case "character varying":
				values = StringUtils.hasText(values) ? values.concat(",\'" + val + "\'")
						: values.concat("\'" + val + "\'");
				break;
			case "numeric":
				values = StringUtils.hasText(values) ? values.concat("," + val) : values.concat(val);
				break;
			case "date":
				values = StringUtils.hasText(values) ? values.concat(",\'" + val + "\'")
						: values.concat("\'" + val + "\'");
				break;
			case "timestamp":
				values = StringUtils.hasText(values) ? values.concat(",\'" + val + "\'")
						: values.concat("\'" + val + "\'");
				break;

			default:
				values = StringUtils.hasText(values) ? values.concat(",\'" + val + "\'")
						: values.concat("\'" + val + "\'");
			}
		}

		values = "(" + values.concat(",\'" + Date.from(Instant.now()) + "\'").concat(",\'" + operation + "\'")
				.concat(",\'system\'") + ")";
		log.info("Columns:{} and Values:{}", columns, values);
		columnMap.put("AUDIT_ADD_DATE", "timestamp");
		columnMap.put("AUDIT_PERFORMED_OPERATION", "character varying(30)");
		columnMap.put("AUDIT_ADD_USERNAME", "character varying(30)");

		if (!availableTables.contains(table + "_log")) {
			createTable(table, columnMap);
		}

		String query = "insert into " + table + "_log" + columns.toLowerCase() + " values " + values;
		log.info(query);
		return query;
	}

	@PostConstruct
	private List<String> listAvailableTablesFromDB() {
		try (Connection dbConnection = getConnection(dbURL, getProperties())) {
			DatabaseMetaData metaData = dbConnection.getMetaData();
			String[] types = { "TABLE" };
			ResultSet rs = metaData.getTables(null, null, "%", types);
			while (rs.next()) {
				// Todo: _log only OR audit schema tables
				System.out.println(rs.getString("TABLE_NAME"));
				availableTables.add(rs.getString("TABLE_NAME"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return availableTables;
	}

	private Properties getProperties() {
		Properties props = new Properties();
		PGProperty.USER.set(props, dbUsername);
		PGProperty.PASSWORD.set(props, dbPasswd);
		PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "11");
		PGProperty.REPLICATION.set(props, "database");
		PGProperty.PREFER_QUERY_MODE.set(props, "simple");
		return props;
	}

	private enum OPERATION {
		insert, update, delete
	}
}
