using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CreditsDataDL
{
    enum EXECUTIONMODE { DOWNLOAD_DELTAS_FROM_BASELINE, DOWNLOAD_FROM_SPLIT_DIFF_FILES, GENERATE_SPLIT_DIFF_FILES, UPDATE_PRE_EXISTING_IDS_FILE, ERROR }

    public class Cast
    {

        [Key, Column(Order = 1), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int cast_id { get; set; }
        [Key, Column(Order = 2), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string character { get; set; }
        public string credit_id { get; set; }
        public int? gender { get; set; }
        [Key, Column(Order = 3), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int id { get; set; }
        public string name { get; set; }
        public int? order { get; set; }
        public string profile_path { get; set; }
        /*[ForeignKey("movie_credits")]
        public int? movie_credits_id;
        public MovieCredits movie_credits;*/
    }

    public class Crew
    {
        [Key, Column(Order = 1), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string credit_id { get; set; }
        public string department { get; set; }
        public int? gender { get; set; }
        [Key, Column(Order = 2), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int id { get; set; }
        public string job { get; set; }
        [Key, Column(Order = 3), DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string name { get; set; }
        public string profile_path { get; set; }
        /*[ForeignKey("movie_credits")]
        public int movie_credits_id;
        public MovieCredits movie_credits;*/
    }

    public class MovieCredits
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int id { get; set; }
        public List<Cast> cast { get; set; }
        public List<Crew> crew { get; set; }
    }

    public class MovieCreditsContext : DbContext
    {
        public MovieCreditsContext() : base()
        {

        }

        public DbSet<MovieCredits> MovieCreditsSet { get; set; }
        public DbSet<Cast> CastSet { get; set; }
        public DbSet<Crew> CrewSet { get; set; }
    }

    public class MovieDataListingEntry
    {
        public bool adult { get; set; }
        public int id { get; set; }
        public string original_title { get; set; }
        public double popularity { get; set; }
        public bool video { get; set; }
    }

    class Program
    {
        static HttpClient movieClient = new HttpClient();
        static bool continueAddingMoviesToDb = true;
        static ConcurrentQueue<MovieCredits> movieCreditsQueue = new ConcurrentQueue<MovieCredits>();

        public static string Decompress(FileInfo fileToDecompress)
        {
            string newFileName = string.Empty;
            using (FileStream originalFileStream = fileToDecompress.OpenRead())
            {
                string currentFileName = fileToDecompress.FullName;
                newFileName = currentFileName.Remove(currentFileName.Length - fileToDecompress.Extension.Length);

                using (FileStream decompressedFileStream = File.Create(newFileName))
                {
                    using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                    {
                        decompressionStream.CopyTo(decompressedFileStream);
                    }
                }
            }

            return newFileName;
        }

        static List<int> GetUpdatedMovieIds(Dictionary<int, int> existingMovieIds)
        {
            HttpResponseMessage response = null;
            DateTime yesterday = DateTime.Today.AddDays(-1);
            using (HttpClient fileClient = new HttpClient())
            {
                string fileUrl = String.Format("http://files.tmdb.org/p/exports/movie_ids_{0}_{1}_{2}.json.gz", yesterday.Month.ToString("00"), yesterday.Day.ToString("00"), yesterday.Year.ToString("00"));
                response = fileClient.GetAsync(fileUrl).Result;
            }

            ConcurrentBag<int> idDeltas;
            if (response != null)
            {
                if (response.IsSuccessStatusCode)
                {
                    string compressedFileName = "IdDelta.gz";
                    Stream streamToReadFrom = response.Content.ReadAsStreamAsync().Result;

                    var fileStream = File.Create(compressedFileName);
                    streamToReadFrom.Seek(0, SeekOrigin.Begin);
                    streamToReadFrom.CopyTo(fileStream);
                    fileStream.Close();
                    streamToReadFrom.Close();

                    FileInfo compressedFile = new FileInfo(compressedFileName);
                    var newFileName = Decompress(compressedFile);

                    var lines = File.ReadLines(newFileName);
                    idDeltas = new ConcurrentBag<int>();
                    Parallel.ForEach(lines, line =>
                    {
                        var movieListingEntry = JsonConvert.DeserializeObject<MovieDataListingEntry>(line);
                        var movieListingEntryId = movieListingEntry.id;
                        if (!existingMovieIds.ContainsKey(movieListingEntryId) && movieListingEntryId != 0)
                        {
                            idDeltas.Add(movieListingEntryId);
                        }
                    });

                }
                else
                {
                    idDeltas = new ConcurrentBag<int>();
                }
            }
            else
            {
                idDeltas = new ConcurrentBag<int>();
            }

            return idDeltas.Distinct().ToList();
        }

        static void SaveMoviesToDB()
        {
            MovieCreditsContext ctx = new MovieCreditsContext();
            int moviesAdded = 0;
            while (continueAddingMoviesToDb || !movieCreditsQueue.IsEmpty)
            {
                while (!movieCreditsQueue.IsEmpty && moviesAdded < 100)
                {
                    MovieCredits movieCredits;
                    if (movieCreditsQueue.TryDequeue(out movieCredits))
                    {
                        ctx.MovieCreditsSet.Add(movieCredits);
                        if (movieCredits.id != 0)
                        {
                            if (movieCredits.cast != null)
                            {
                                /*foreach (var castMember in movieCredits.cast)
                                {
                                    //castMember.movie_credits_id = movieCredits.id;
                                    castMember.movie_credits = movieCredits;
                                }*/
                                ctx.CastSet.AddRange(movieCredits.cast);
                                /*if(movieCredits.cast.Count > 0)
                                    Console.WriteLine($"ID BEFORE SAVE: {movieCredits.id}, with character name: {movieCredits.cast[0].character}");*/
                            }

                            if (movieCredits.crew != null)
                            {
                                /*foreach (var crewMember in movieCredits.crew)
                                {
                                    //crewMember.movie_credits_id = movieCredits.id;
                                    crewMember.movie_credits = movieCredits;
                                }*/
                                ctx.CrewSet.AddRange(movieCredits.crew);
                            }
                            moviesAdded += 1;
                            ctx.SaveChanges();
                        }
                    }
                }

                if (moviesAdded > 0)
                {
                    Console.WriteLine("Processed Another: {0}, {1}     ", moviesAdded, DateTime.Now.ToLocalTime());
                }

                moviesAdded = 0;
                ctx = new MovieCreditsContext();
            }

            Console.WriteLine("THIS SHOULD HAPPEN AT END");
            Console.WriteLine("THIS SHOULD HAPPEN AT END");
        }

        static async Task<MovieCredits> GetMovieAsync(string path)
        {
            MovieCredits movieCredits = null;

            HttpResponseMessage response = await movieClient.GetAsync(path);
            if (response.IsSuccessStatusCode)
            {
                movieCredits = await response.Content.ReadAsAsync<MovieCredits>();
            }

            return movieCredits;
        }

        static async Task RetrieveWriteMovie(double movieId, string apiKey)
        {
            var getMovieString = String.Format("movie/{0}/credits?api_key={1}&language=en-US%27", movieId, apiKey);
            try
            {
                MovieCredits movie = await GetMovieAsync(getMovieString);

                if (movie != null)
                {
                    movieCreditsQueue.Enqueue(movie);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Movie failure: {movieId}");
                Console.WriteLine($"Exception is: {ex.Message}");
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Working...");

            string apiKey = args[0];
            string movieSplitFileName = "MovieListSplit";

            var mode = EXECUTIONMODE.DOWNLOAD_DELTAS_FROM_BASELINE;
            if (args.Length > 1)
            {
                int modeNum = -1;
                int.TryParse(args[1], out modeNum);

                if (modeNum == 1)
                    mode = EXECUTIONMODE.GENERATE_SPLIT_DIFF_FILES;
                else if (modeNum == 2)
                    mode = EXECUTIONMODE.UPDATE_PRE_EXISTING_IDS_FILE;
                else if (modeNum == 3)
                    mode = EXECUTIONMODE.DOWNLOAD_FROM_SPLIT_DIFF_FILES;
            }

            string preExistingFileIdsFileName = "preExistingIds";

            var filePreExistingIds = File.Exists(preExistingFileIdsFileName) ? File.ReadAllLines(preExistingFileIdsFileName).Select(idEntry => int.Parse(idEntry)) : new List<int>();

            MovieCreditsContext ctx = new MovieCreditsContext();
            var existingMovieIds = ctx.MovieCreditsSet.Select(movie => movie.id).ToList();

            var allPreExistingMovieIds = existingMovieIds.Union(filePreExistingIds).Distinct().ToDictionary(idEntry => idEntry);

            if (mode == EXECUTIONMODE.UPDATE_PRE_EXISTING_IDS_FILE)
            {
                File.WriteAllLines(preExistingFileIdsFileName, allPreExistingMovieIds.Select(entry => entry.Key.ToString()));
            }

            if (mode == EXECUTIONMODE.GENERATE_SPLIT_DIFF_FILES)
            {
                var allMovieDeltas = GetUpdatedMovieIds(allPreExistingMovieIds);

                int numSplits = -1;
                if (int.TryParse(args[2], out numSplits))
                {
                    for (int i = 0; i < numSplits; i++)
                    {
                        TextWriter tw = new StreamWriter($"{movieSplitFileName + i}.txt");

                        var finalIndexNum = -1;
                        if (i == numSplits - 1)
                            finalIndexNum = allMovieDeltas.Count - 1;
                        else
                            finalIndexNum = (allMovieDeltas.Count / numSplits) * (i + 1);

                        for (int j = i * allMovieDeltas.Count / numSplits; j < finalIndexNum; j++)
                        {
                            tw.WriteLine(allMovieDeltas[j]);
                        }

                        tw.Close();
                    }
                }
            }

            List<int> movieIdDeltas = new List<int>();

            if (mode == EXECUTIONMODE.DOWNLOAD_DELTAS_FROM_BASELINE)
            {
                movieIdDeltas = GetUpdatedMovieIds(allPreExistingMovieIds);
                Console.WriteLine($"Num Movies Missing: { movieIdDeltas.Count }");
            }

            if (mode == EXECUTIONMODE.DOWNLOAD_FROM_SPLIT_DIFF_FILES)
            {
                int splitDiffFileNum = -1;

                if (int.TryParse(args[2], out splitDiffFileNum))
                {
                    var splitFileIds = File.ReadAllLines(movieSplitFileName + splitDiffFileNum + ".txt").Select(id => int.Parse(id));
                    Parallel.ForEach(splitFileIds, splitFileId =>
                    {
                        if (!allPreExistingMovieIds.ContainsKey(splitFileId))
                        {
                            movieIdDeltas.Add(splitFileId);
                        }
                    });
                    Console.WriteLine($"Num Movies Missing: { movieIdDeltas.Count }");
                }
                else
                {
                    mode = EXECUTIONMODE.ERROR;
                    Console.WriteLine("Please provide a command argument indicating which Movie Split File Number to parse");
                }
            }

            if (mode == EXECUTIONMODE.DOWNLOAD_DELTAS_FROM_BASELINE || mode == EXECUTIONMODE.DOWNLOAD_FROM_SPLIT_DIFF_FILES)
            {
                Console.WriteLine("Beginning to download");
                movieClient.BaseAddress = new Uri("https://api.themoviedb.org/3/");
                movieClient.DefaultRequestHeaders.Accept.Clear();
                movieClient.DefaultRequestHeaders.Accept.Add(
                    new MediaTypeWithQualityHeaderValue("application/json"));

                var saveMoviesTask = Task.Run(() => SaveMoviesToDB());

                int rateLimitPeriodSeconds = 10;
                TimeSpan rateLimitSecondSpan = new TimeSpan(0, 0, rateLimitPeriodSeconds);
                int rateLimitPerPeriod = 30;
                var queries = new Task[rateLimitPerPeriod];
                DateTime firstQueryTime;
                for (int i = 0; i < movieIdDeltas.Count; i += rateLimitPerPeriod)
                {
                    for (int requestIdx = 0; requestIdx < rateLimitPerPeriod; requestIdx++)
                    {
                        queries[requestIdx] = RetrieveWriteMovie(movieIdDeltas[i + requestIdx], apiKey);
                    }
                    firstQueryTime = DateTime.Now;

                    Task.WaitAll(queries);
                    queries = new Task[rateLimitPerPeriod];

                    var elapsedTimeSinceFirstQuery = DateTime.Now - firstQueryTime;
                    //Console.WriteLine($"Elapsed Time Since Query: {elapsedTimeSinceFirstQuery}");
                    if (elapsedTimeSinceFirstQuery.TotalSeconds < rateLimitPerPeriod)
                    {
                        var sleepTime = (int)(rateLimitSecondSpan - elapsedTimeSinceFirstQuery).TotalMilliseconds;
                        if (sleepTime > 0)
                        {
                            //Console.WriteLine($"Sleeping for {sleepTime} Milliseconds");
                            Thread.Sleep(sleepTime);
                        }
                    }
                    /*if (i % 100 == 0)
                    {
                        Console.WriteLine($"Another 100 queries sent off: {DateTime.Now.ToString()}");
                    }*/

                    while (movieCreditsQueue.Count > 300)
                    {
                        Thread.Sleep(4000);
                    }
                }

                while (movieCreditsQueue.Count > 0)
                {
                    Thread.Sleep(4000);
                }

                continueAddingMoviesToDb = false;
                saveMoviesTask.Wait();
            }
            Console.WriteLine("Finished");
            Console.ReadKey();
        }
    }
}
