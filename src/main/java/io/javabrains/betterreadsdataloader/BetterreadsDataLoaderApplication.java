package io.javabrains.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import connection.DataStaxAstraProperties;
import io.javabrains.betterreadsdataloader.author.Author;
import io.javabrains.betterreadsdataloader.author.AuthorRepository;
import io.javabrains.betterreadsdataloader.book.Book;
import io.javabrains.betterreadsdataloader.book.BookRepository;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.authors.path}")
	private String authorPath;

	@Value("${datadump.books.path}")
	private String booksPath;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start() {
		// dumpAuthors();
		dumpBooks();
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

	private void dumpBooks() {
		Path path = Paths.get(booksPath);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				try {
					JsonObject jsonObject = JsonParser.parseString(line.substring(line.indexOf("{"))).getAsJsonObject();

					Book book = new Book();
					book.setId(jsonObject.get("key").getAsString().replace("/works/", ""));

					var bookTitle = jsonObject.get("title");
					book.setName(bookTitle == null ? "" : bookTitle.getAsString());

					JsonElement description = jsonObject.get("description");
					if (description != null) {
						var desc = description.getAsJsonObject().get("value");
						book.setDescription(desc != null ? desc.getAsString() : "");
					}

					JsonElement createdDate = jsonObject.get("created");
					if (createdDate != null) {
						String date = createdDate.getAsJsonObject().get("value").getAsString();
						if (date != null) {
							book.setPublishedDate(LocalDate.parse(date, dateFormat));
						}
					}

					JsonArray covers = jsonObject.get("covers").getAsJsonArray();
					if (covers != null) {
						List<String> coverIds = new ArrayList<>();
						Iterator<JsonElement> iterator = covers.iterator();
						while (iterator.hasNext()) {
							coverIds.add(iterator.next().getAsString());
						}
						book.setCoverIds(coverIds);
					}

					JsonArray authorJsonElement = jsonObject.getAsJsonArray("authors");
					if (authorJsonElement != null) {
						List<String> authors = new ArrayList<>();
						Iterator<JsonElement> iterator = authorJsonElement.iterator();
						while (iterator.hasNext()) {
							JsonObject asJsonObject = iterator.next().getAsJsonObject().get("author").getAsJsonObject();
							var key = asJsonObject.get("key");
							if (key != null) {
								authors.add(key.getAsString().replace("/authors/", ""));
							}
						}
						book.setAuthorids(authors);

						book.setAuthorNames(authors.stream().map(author -> authorRepository.findById(author))
								.map(author -> {
									if (author.isPresent()) {
										return author.get().getName();
									}
									return "unknown";
								}).collect(Collectors.toList()));
					}
					System.out.println("Books added " + book.getId() + "...");
					bookRepository.save(book);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Bean
	CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path path = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(path);
	}

}
