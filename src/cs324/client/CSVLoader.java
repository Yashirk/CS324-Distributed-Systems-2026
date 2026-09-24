package cs324.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVLoader {

    private CSVLoader() {
    }

    public static List<Integer> loadNumbers(
            File file)
            throws IOException {

        String content =
                Files.readString(
                        file.toPath()
                );

        List<Integer> numbers =
                new ArrayList<>();

        Pattern pattern =
                Pattern.compile(
                        "-?\\d+"
                );

        Matcher matcher =
                pattern.matcher(
                        content
                );

        while (matcher.find()) {

            numbers.add(
                    Integer.parseInt(
                            matcher.group()
                    )
            );
        }

        if (numbers.isEmpty()) {

            throw new IOException(
                    "CSV file contains no valid numbers."
            );
        }

        return numbers;
    }
}