package programmer_zama_now.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.data.redis.support.collections.RedisZSet;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;


@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ProductService productService;

    @Test
    void redisTemplate() {
        Assertions.assertNotNull(redisTemplate);
    }

    @Test
    void string() throws InterruptedException{
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "Eko", Duration.ofSeconds(2));
        Assertions.assertEquals("Eko", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        Assertions.assertNull(operations.get("name"));
    }

    @Test
    void list() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("names", "Eko");
        operations.rightPush("names", "Kurniawan");
        operations.rightPush("names", "Khaneddy");

        Assertions.assertEquals("Eko", operations.leftPop("names"));
        Assertions.assertEquals("Kurniawan", operations.leftPop("names"));
        Assertions.assertEquals("Khaneddy", operations.leftPop("names"));
    }

    @Test
    void set() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("students", "Eko");
        operations.add("students", "Eko");
        operations.add("students", "Kurniawan");
        operations.add("students", "Kurniawan");
        operations.add("students", "Khaneddy");
        operations.add("students", "Khaneddy");

        Set<String> students = operations.members("students");
        Assertions.assertEquals(3, students.size());
        assertThat(students, hasItems("Eko", "Kurniawan", "Khaneddy"));
    }

    @Test
    void zSet() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();

        operations.add("score", "Eko", 100);
        operations.add("score", "Budi", 85);
        operations.add("score", "Joko", 90);

        Assertions.assertEquals("Eko", operations.popMax("score").getValue());
        Assertions.assertEquals("Joko", operations.popMax("score").getValue());
        Assertions.assertEquals("Budi", operations.popMax("score").getValue());
    }

    @Test
    void hash() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();
        operations.put("user:1", "id", "1");
        operations.put("user:1", "name", "Eko");
        operations.put("user:1", "email", "eko@example.com");

        Assertions.assertEquals("1", operations.get("user:1", "id"));
        Assertions.assertEquals("Eko", operations.get("user:1", "name"));
        Assertions.assertEquals("eko@example.com", operations.get("user:1", "email"));

        redisTemplate.delete("user:1");
    }

//    @Test
//    void geo() {
//        GeoOperations<String, String> operations = redisTemplate.opsForGeo();
//        operations.add("sellers", new Point(106.822702, -6.177590), "Toko A");
//        operations.add("sellers", new Point(106.820889, -6.174964), "Toko B");
//
//        Distance distance = operations.distance("sellers", "Toko A", "Toko B", Metrics.KILOMETERS);
//        Assertions.assertEquals(0.3543, distance.getValue());
//
//        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers = operations
//                .search("sellers", new Circle(
//                new Point(106.821825, -6.175105),
//                new Distance(5, Metrics.KILOMETERS)));
//
//        Assertions.assertEquals(2, sellers.getContent().size());
//        Assertions.assertEquals("Toko A", sellers.getContent().get(0).getContent().getName());
//        Assertions.assertEquals("Toko B", sellers.getContent().get(1).getContent().getName());
//    }

    @Test
    void hyperloglog() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();
        operations.add("traffics", "eko", "kurniawan", "khaneddy");
        operations.add("traffics", "eko", "budi", "joko");
        operations.add("traffics", "budi", "joko", "ruli");

        Assertions.assertEquals(6L, operations.size("traffics"));
    }

    @Test
    void transaction() {
        redisTemplate.execute(new SessionCallback<Object>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();

                operations.opsForValue().set("test1", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Budi", Duration.ofSeconds(2));

                operations.exec();
                return null;
            }
        });

        Assertions.assertEquals("Eko", redisTemplate.opsForValue().get("test1"));
        Assertions.assertEquals("Budi", redisTemplate.opsForValue().get("test2"));
    }
    
    @Test
    void pipeline() {
        List<Object> statuses = redisTemplate.executePipelined(new SessionCallback<>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test3", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test4", "Eko", Duration.ofSeconds(2));
                return null;
            }
        });

        assertThat(statuses, hasSize(4));
        assertThat(statuses, hasItems(true));
        assertThat(statuses, not(hasItems(false)));
    }

    @Test
    void publishStream() {
         var operations = redisTemplate.opsForStream();
         var record = MapRecord.create("stream-1", Map.of(
                 "name", "Eko Lurniawan",
                 "address", "Indonesia"
         ));

        for (int i = 0; i < 10; i++) {

            operations.add(record);
        }
    }

    @Test
    void subscribe() {
        var operations = redisTemplate.opsForStream();

        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {

        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"), StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }

    @Test
    void pubSub() {
        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());

                System.out.println("Received message: " + event);
            }
        }, "my-channel".getBytes());

        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());

                System.out.println("Received message: " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            redisTemplate.convertAndSend("my-channel", "Hello World: " + i);
        }
    }

    @Test
    void redisList() {
        List<String> list = RedisList.create("names", redisTemplate);
        list.add("Eko");
        list.add("Kurniawan");
        list.add("Khannedy");

        assertThat(list, hasItems("Eko", "Kurniawan", "Khannedy"));

        List<String> result = redisTemplate.opsForList().range("names", 0, -1);

        assertThat(result, hasItems("Eko", "Kurniawan", "Khannedy"));
    }

    @Test
    void redisSet() {
        Set<String> set = RedisSet.create("traffic", redisTemplate);
        set.addAll(Set.of("eko", "kurniawan", "khannedy"));
        set.addAll(Set.of("Eko", "budi", "rully"));
        set.addAll(Set.of("joko", "budi", "rully"));

        assertThat(set, hasItems("eko", "kurniawan", "budi", "rully", "joko"));

        Set<String> members = redisTemplate.opsForSet().members("traffic");
        assertThat(members, hasItems("eko", "kurniawan", "budi", "rully", "joko"));
    }

    @Test
    void rediSZet() {
        RedisZSet<String> set = RedisZSet.create("winner", redisTemplate);
        set.add("Eko", 100);
        set.add("Budi", 85);
        set.add("Joko", 90);

        assertThat(set, hasItems("Eko", "Budi", "Joko"));

        Set<String> winner = redisTemplate.opsForZSet().range("winner", 0, -1);
        assertThat(winner, hasItems("Eko", "Budi", "Joko"));

        Assertions.assertEquals("Eko", set.popLast());
        Assertions.assertEquals("Joko", set.popLast());
        Assertions.assertEquals("Budi", set.popLast());
    }

    @Test
    void redisMap() {
        Map<String, String> map = new DefaultRedisMap<>("user:1", redisTemplate);
        map.put("name", "Eko");
        map.put("address", "Indonesia");

        assertThat(map, hasEntry("name", "Eko"));
        assertThat(map, hasEntry("address", "Indonesia"));

        Map<Object, Object> entries = redisTemplate.opsForHash().entries("user:1");
        assertThat(entries, hasEntry("name", "Eko"));
        assertThat(entries, hasEntry("address", "Indonesia"));
    }

    @Test
    void redisRepository() {
        Product product = Product.builder()
                .id("1")
                .name("Mie Ayam Goreng")
                .price(20_000L)
                .build();
        productRepository.save(product);

        Product product2 = productRepository.findById("1").get();
        Assertions.assertEquals(product, product2);

        Map<Object, Object> map = redisTemplate.opsForHash().entries("products:1");
        Assertions.assertEquals(product.getId(), map.get("id"));
        Assertions.assertEquals(product.getName(), map.get("name"));
        Assertions.assertEquals(product.getPrice().toString(), map.get("price"));

    }

    @Test
    void ttl() throws  InterruptedException {
        Product product = Product.builder()
                .id("1")
                .name("Mie Ayam Goreng")
                .price(20_000L)
                .ttl(3L)
                .build();
        productRepository.save(product);

        Assertions.assertTrue(productRepository.findById("1").isPresent());

        Thread.sleep(Duration.ofSeconds(5));

        Assertions.assertFalse(productRepository.findById("1").isPresent());
    }

    @Test
    void cache() {
        Cache sample = cacheManager.getCache("scores");
        sample.put("Eko", 100);
        sample.put("Budi", 95);

        Assertions.assertEquals(100, sample.get("Eko", Integer.class));
        Assertions.assertEquals(95, sample.get("Budi", Integer.class));

        sample.evict("Eko");
        sample.evict("Budi");
        Assertions.assertNull(sample.get("Eko", Integer.class));
        Assertions.assertNull(sample.get("Budi", Integer.class));
    }

    @Test
    void findProduct() {
        Product product = productService.getProduct("P-001");
        Assertions.assertNotNull(product);
        Assertions.assertEquals("P-001", product.getId());
        Assertions.assertEquals("Sample", product.getName());

        Product product2 = productService.getProduct("P-001");
        Assertions.assertEquals(product, product2);
    }

    @Test
    void saveProduct() {
        Product product = Product.builder().id("P-002").name("Sample").build();
        productService.save(product);

        Product product2 = productService.getProduct("P-002");
        Assertions.assertEquals(product, product2);
    }

    @Test
    void remove() {
        Product product = productService.getProduct("P-003");
        Assertions.assertNotNull(product);

        productService.remove("P-003");

        Product product2 = productService.getProduct("P-003");
        Assertions.assertEquals(product, product2);
    }
}
