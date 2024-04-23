package io.javabrains.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.json.GsonJsonParser;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.internal.core.util.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import connection.DataStaxAstraProperties;
import io.javabrains.betterreadsdataloader.author.Author;
import io.javabrains.betterreadsdataloader.author.AuthorRepository;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Value("${datadump.authors.path}")
	private String authorPath;

	@Value("${datadump.books.path}")
	private String booksPath;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start() {
		dumpAuthors();
	}

	private void dumpAuthors() {
		Path path = Paths.get(authorPath);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(rawauthor -> {
				JsonObject jsonObject = JsonParser.parseString(rawauthor.substring(rawauthor.indexOf("{")))
						.getAsJsonObject();
				Author author = new Author();
				author.setName(jsonObject.get("name").getAsString());
				JsonElement personalName = jsonObject.get("personal_name");
				author.setPersonalName(personalName == null ? "" : personalName.getAsString());
				author.setId(jsonObject.get("key").getAsString().replace("/authors/", ""));
				System.out.println("Creating author " + author.getName() + "...");
				authorRepository.save(author);
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		;
	}

	@Bean
	CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path path = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(path);
	}

}
