package how.hollow;

public class DataModelSchema {

    public static final String MOVIE_SCHEMA = "Movie @PrimaryKey(id) {\n" +
            "    long id;\n" +
            "    int releaseYear;\n" +
            "    string title;\n" +
            "}";

    public static final String ALL_SCHEMA = "Movie @PrimaryKey(id) {\n" +
            "    long id;\n" +
            "    int releaseYear;\n" +
            "    string title;\n" +
            "    SetOfActor actors;\n" +
            "}\n" +
            "\n" +
            "SetOfActor Set<Actor> @HashKey(firstname, surname);\n" +
            "\n" +
            "Actor {\n" +
            "    long id;\n" +
            "    String firstname;\n" +
            "    String surname;\n" +
            "}\n" +
            "\n" +
            "String {\n" +
            "    string value;\n" +
            "}\n";
}
