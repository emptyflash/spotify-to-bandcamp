const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const bandcamp = require('bandcamp-scraper');

// Input and Output CSV paths
const INPUT_CSV = 'input.csv'; // Replace with your input CSV
const OUTPUT_CSV = 'output.csv';

// Rate limit and backoff configuration
const RATE_LIMIT_MS = 1000; // 1 request per second
const MAX_RETRIES = 5; // Maximum retries for a failed request
const BACKOFF_FACTOR = 2; // Exponential backoff multiplier

// Utility function: delay for a given duration
function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Clean up Row Key to remove garbage characters (e.g., BOM)
function cleanKey(key) {
    return key.replace(/^\uFEFF/, '').trim();
}

// Function to group tracks by Artist, then Album
function groupByArtistAndAlbum(tracks) {
    const grouped = {};
    tracks.forEach((track) => {
        const artist = track['Artist Name(s)'];
        const album = track['Album Name'];
        if (!grouped[artist]) {
            grouped[artist] = {};
        }
        if (!grouped[artist][album]) {
            grouped[artist][album] = [];
        }
        grouped[artist][album].push(track);
    });
    return grouped;
}

// Retry wrapper with exponential backoff
async function retryWithBackoff(fn, retries = MAX_RETRIES, delayMs = RATE_LIMIT_MS) {
    let attempt = 0;
    while (attempt < retries) {
        try {
            const result = await fn();
            if (result !== null) return result; // Return if valid result
            return null; // No retries for empty search results
        } catch (error) {
            console.error(`  Error: ${error.message}. Retrying in ${delayMs} ms...`);
            await delay(delayMs);
            delayMs *= BACKOFF_FACTOR; // Exponential backoff
            attempt++;
        }
    }
    console.error(`  Request failed after ${retries} retries.`);
    return null;
}

// Fetch all albums for an artist
async function fetchArtistAlbums(artist) {
    console.log(`  Fetching albums for artist: "${artist}"...`);
    return retryWithBackoff(() =>
        new Promise((resolve) => {
            bandcamp.search({ query: artist, page: 1 }, (error, searchResults) => {
                if (error) {
                    throw new Error(`Failed to search for artist: ${artist}`);
                }
                const artistResult = searchResults?.find((res) => res.type === 'artist');
                if (!artistResult) {
                    console.log(`  No artist page found for: "${artist}".`);
                    resolve(null); // No retries for missing artist
                    return;
                }

                bandcamp.getAlbumUrls(artistResult.url, (error, albumList) => {
                    if (error) {
                        throw new Error(`Failed to fetch albums for artist: ${artist}`);
                    }
                    resolve(albumList || []); // Return empty if no albums
                });
            });
        })
    );
}

// Fetch album tracks
async function fetchAlbumTracks(albumUrl) {
    return retryWithBackoff(() =>
        new Promise((resolve) => {
            bandcamp.getAlbumInfo(albumUrl, (error, albumInfo) => {
                if (error) {
                    throw new Error(`Failed to fetch tracks for album: ${albumUrl}`);
                }
                const tracksMap = albumInfo.tracks.map((track) => ({
                    trackName: track.name,
                    trackLink: track.url
                }));
                resolve(tracksMap);
            });
        })
    );
}

// Search for a specific track using artist and track name
async function fetchMatchingTrackUrl(artist, trackName) {
    console.log(`    Searching for track: "${trackName}" by "${artist}"...`);
    return retryWithBackoff(() =>
        new Promise((resolve) => {
            bandcamp.search({ query: `${artist} ${trackName}`, page: 1 }, (error, searchResults) => {
                if (error) {
                    throw new Error(`Failed to search for track: "${trackName}" by "${artist}"`);
                }
                const trackResult = searchResults?.find((res) => res.type === 'track');
                resolve(trackResult ? trackResult.url : null);
            });
        })
    );
}

// Process CSV file
async function processCSV() {
    const tracks = [];

    // Parse CSV
    console.log('Parsing input CSV...');
    await new Promise((resolve) => {
        fs.createReadStream(INPUT_CSV)
            .pipe(csv())
            .on('data', (row) => {
                const cleanedRow = {};
                Object.keys(row).forEach((key) => {
                    cleanedRow[cleanKey(key)] = row[key];
                });
                tracks.push(cleanedRow);
            })
            .on('end', resolve);
    });

    const groupedArtists = groupByArtistAndAlbum(tracks);
    console.log(`Found ${Object.keys(groupedArtists).length} artists.`);

    // Setup CSV Writer
    const csvWriter = createCsvWriter({
        path: OUTPUT_CSV,
        append: fs.existsSync(OUTPUT_CSV),
        header: [
            { id: 'Track ID', title: 'Track ID' },
            { id: 'Track Name', title: 'Track Name' },
            { id: 'Album Name', title: 'Album Name' },
            { id: 'Artist Name', title: 'Artist Name' },
            { id: 'Bandcamp Link', title: 'Bandcamp Link' },
            { id: 'Spotify Added At', title: 'Spotify Added At' }
        ]
    });

    let processedArtists = 0;

    for (const artist in groupedArtists) {
        processedArtists++;
        console.log(`\n[${processedArtists}/${Object.keys(groupedArtists).length}] Processing artist: "${artist}"`);

        // Fetch albums for the artist
        const artistAlbums = await fetchArtistAlbums(artist);
        const albumMap = artistAlbums
            ? new Map(artistAlbums.map((album) => [album.title.toLowerCase(), album.url]))
            : new Map();

        for (const album in groupedArtists[artist]) {
            console.log(`  Processing album: "${album}"...`);
            const albumUrl = albumMap.get(album.toLowerCase());

            let bandcampTracks = [];
            if (albumUrl) {
                bandcampTracks = await fetchAlbumTracks(albumUrl) || [];
            } else {
                console.log(`    Album "${album}" not found. Falling back to track search.`);
            }

            const outputData = await Promise.all(
                groupedArtists[artist][album].map(async (track) => {
                    let trackLink = null;
                    if (bandcampTracks.length > 0) {
                        const matchingTrack = bandcampTracks.find(
                            (t) => t.trackName.toLowerCase() === track['Track Name'].toLowerCase()
                        );
                        trackLink = matchingTrack ? matchingTrack.trackLink : null;
                    } else {
                        trackLink = await fetchMatchingTrackUrl(artist, track['Track Name']);
                    }

                    return {
                        'Track ID': track['Track ID'],
                        'Track Name': track['Track Name'],
                        'Album Name': track['Album Name'],
                        'Artist Name': track['Artist Name(s)'],
                        'Bandcamp Link': trackLink || 'Not Found',
                        'Spotify Added At': track['Added At']
                    };
                })
            );

            await csvWriter.writeRecords(outputData);
            console.log(`    Saved ${outputData.length} track(s) to output CSV.`);
            await delay(RATE_LIMIT_MS); // Rate limiting
        }
    }

    console.log(`\nProcessing complete! Results saved to ${OUTPUT_CSV}`);
}

processCSV();
