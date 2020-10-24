
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ComputeEngineSample {

  private static final String APPLICATION_NAME = "MapReduce";
 private static final String PROJECT_ID = "shruti-desai";
 private static final String ZONE_NAME = "us-central1-a";
 private static final String SAMPLE_INSTANCE_NAME = "keyvaluestorepack";
 private static final String SOURCE_IMAGE_PREFIX =
      "https://www.googleapis.com/compute/v1/projects/";

  private static final String SOURCE_IMAGE_PATH =
      "ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20200529";
 private static final String NETWORK_INTERFACE_CONFIG = "ONE_TO_ONE_NAT";

  private static final String NETWORK_ACCESS_CONFIG = "External NAT";
 private static final long OPERATION_TIMEOUT_MILLIS = 60 * 1000;
 private static HttpTransport httpTransport;
 private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  public static void main(String[] args) {
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      String jsonPath ="C:\\Users\\Shraddha\\Desktop\\Shruti\\Courses\\CloudComputing\\shruti-desai-1dc41788adaf.json";
      GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(jsonPath));
      if (credential.createScopedRequired()) {
        List<String> scopes = new ArrayList<>();
        scopes.add(ComputeScopes.DEVSTORAGE_FULL_CONTROL);
        scopes.add(ComputeScopes.COMPUTE);
        credential = credential.createScoped(scopes);
      }
      HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credential);
      Compute compute =
          new Compute.Builder(httpTransport, JSON_FACTORY, requestInitializer)
              .setApplicationName(APPLICATION_NAME)
              .build();

      boolean foundOurInstance = printInstances(compute);

      Operation op;
      if (foundOurInstance) {
        op = deleteInstance(compute, SAMPLE_INSTANCE_NAME);
      } else {
        op = startInstance(compute, SAMPLE_INSTANCE_NAME);
      }
      System.out.println("Waiting for operation completion...");
      Operation.Error error = blockUntilComplete(compute, op, OPERATION_TIMEOUT_MILLIS);
      if (error == null) {
        System.out.println("Success!");
      } else {
        System.out.println(error.toPrettyString());
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(1);
  }

  public static boolean printInstances(Compute compute) throws IOException {
    System.out.println("================== Listing Compute Engine Instances ==================");
    Compute.Instances.List instances = compute.instances().list(PROJECT_ID, ZONE_NAME);
    InstanceList list = instances.execute();
    boolean found = false;
    if (list.getItems() == null) {
      System.out.println(
          "No instances found.");
    } else {
      for (Instance instance : list.getItems()) {
    	  
        System.out.println(instance.toPrettyString());
        if (instance.getName().equals(SAMPLE_INSTANCE_NAME)) {
          found = true;
        }
      }
    }
    return found;
  }
  public static Operation startInstance(Compute compute, String instanceName) throws IOException {
    System.out.println("================== Starting New Instance ==================");

    Instance instance = new Instance();
    instance.setName(instanceName);
    instance.setMachineType(
        String.format(
            "https://www.googleapis.com/compute/v1/projects/%s/zones/%s/machineTypes/e2-standard-2",
            PROJECT_ID, ZONE_NAME));
    NetworkInterface ifc = new NetworkInterface();
    ifc.setNetwork(
        String.format(
            "https://www.googleapis.com/compute/v1/projects/%s/global/networks/default",
            PROJECT_ID));
    List<AccessConfig> configs = new ArrayList<>();
    AccessConfig config = new AccessConfig();
    config.setType(NETWORK_INTERFACE_CONFIG);
    config.setName(NETWORK_ACCESS_CONFIG);
    configs.add(config);
    ifc.setAccessConfigs(configs);
    instance.setNetworkInterfaces(Collections.singletonList(ifc));

    AttachedDisk disk = new AttachedDisk();
    disk.setBoot(true);
    disk.setAutoDelete(true);
    disk.setType("PERSISTENT");
    AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
    params.setDiskName(instanceName);
    params.setSourceImage(SOURCE_IMAGE_PREFIX + SOURCE_IMAGE_PATH);
    params.setDiskType(
        String.format(
            "https://www.googleapis.com/compute/v1/projects/%s/zones/%s/diskTypes/pd-standard",
            PROJECT_ID, ZONE_NAME));
    disk.setInitializeParams(params);
    instance.setDisks(Collections.singletonList(disk));

    ServiceAccount account = new ServiceAccount();
    account.setEmail("default");
    List<String> scopes = new ArrayList<>();
    scopes.add("https://www.googleapis.com/auth/devstorage.full_control");
    scopes.add("https://www.googleapis.com/auth/compute");
    account.setScopes(scopes);
    instance.setServiceAccounts(Collections.singletonList(account));
Metadata meta = new Metadata();
    Metadata.Items item = new Metadata.Items();
    item.setKey("startup-script-url");
   
    item.setValue(String.format("gs://shruti-desai/keystore.sh", PROJECT_ID));
    meta.setItems(Collections.singletonList(item));
    instance.setMetadata(meta);

    System.out.println(instance.toPrettyString());
    Compute.Instances.Insert insert = compute.instances().insert(PROJECT_ID, ZONE_NAME, instance);
    return insert.execute();
  }
  

  private static Operation deleteInstance(Compute compute, String instanceName) throws Exception {
    System.out.println(
        "================== Deleting Instance " + instanceName + " ==================");
    Compute.Instances.Delete delete =
        compute.instances().delete(PROJECT_ID, ZONE_NAME, instanceName);
    return delete.execute();
  }

  public static Operation.Error blockUntilComplete(
      Compute compute, Operation operation, long timeout) throws Exception {
    long start = System.currentTimeMillis();
    final long pollInterval = 5 * 1000;
    String zone = operation.getZone(); // null for global/regional operations
    if (zone != null) {
      String[] bits = zone.split("/");
      zone = bits[bits.length - 1];
    }
    String status = operation.getStatus();
    String opId = operation.getName();
    while (operation != null && !status.equals("DONE")) {
      Thread.sleep(pollInterval);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= timeout) {
        throw new InterruptedException("Timed out waiting for operation to complete");
      }
      System.out.println("waiting...");
      if (zone != null) {
        Compute.ZoneOperations.Get get = compute.zoneOperations().get(PROJECT_ID, zone, opId);
        operation = get.execute();
      } else {
        Compute.GlobalOperations.Get get = compute.globalOperations().get(PROJECT_ID, opId);
        operation = get.execute();
      }
      if (operation != null) {
        status = operation.getStatus();
      }
    }
    return operation == null ? null : operation.getError();
  }

}