import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import java.util.Properties

import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}


object HasPII {

  def main(args: Array[String]): Unit = {
 
    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.
    val session = Session.builder.configFile("snowflake_connection.properties").create

    // Import names from the implicits object, which allows you to use shorthand to refer to columns in a DataFrame
    // (e.g. `'columnName` and `$"columnName"`).
    import session.implicits._

    Console.println("\n=== Setting up the DataFrame for the data in the stage ===\n")
    // Define the schema for the CSV file containing the demo data.
    val schema = Seq(
      StructField("target", StringType),
      StructField("ids", StringType),
      StructField("date", StringType),
      StructField("flag", StringType),
      StructField("user", StringType),
      StructField("text", StringType),
    )
    // Read data from the demo file in the stage into a Snowpark DataFrame.
    // UDFDemoSetup.dataStageName is the name of the stage that was created when you ran
    // UDFDemoSetup earlier, and UDFDemoSetup.dataFilePattern is the pattern matching the files
    // that were uploaded to that stage.
    val origData = session
      .read
      .schema(StructType(schema))
      .option("compression", "gzip")
      .option("validate_utf8","false")
      .csv(s"@${UDFDemoSetup.dataStageName}/${UDFDemoSetup.dataFilePattern}")
    // Drop all of the columns except the column containing the text of the tweet
    // and return the first 100 rows.
    val tweetData = origData.drop('target, 'ids, 'date, 'flag, 'user).limit(100000)

    Console.println("\n=== Retrieving the data and printing the text of the first 10 tweets")
    // Display some of the data.
    tweetData.show()
    tweetData.write.mode(Overwrite).saveAsTable("tweets")

session.sql("describe table tweets").show()
val tweets = session.table("tweets")

//housekeeping
val cleaned = tweets.where('text.is_not_null)

class PII {
    val targets = Array(
                  "\\d{3}-\\d{2}-\\d{4}",                 // SSN
                  "[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}",  // email
                  "[2-9]\\d{2}-\\d{3}-\\d{4}"             // phone

    )

    val patterns = targets.map(r => Pattern.compile(r))

    def hasPii(s: String):Boolean = {

        patterns.map(_.matcher(s).find()).fold(false)(_ || _)
    }

}

lazy val pii = new PII()

val checkPii = udf((x:String) => pii.hasPii(x))

val withPii = cleaned.where(checkPii('text))
withPii.show()
withPii.write.mode(SaveMode.Append).saveAsTable("pii_tweets")

  }
}

