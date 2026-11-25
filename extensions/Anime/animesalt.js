/**
 * StrawVerse Extension - AnimeSalt Scraper & Multi-Source Resolver
 * Specializing in Multi-Language Dubbed & Subbed Anime (Hindi, Tamil, Telugu, Malayalam, English, Japanese)
 * Integrates both https://animesalt.cx/ and https://animesalt.ro/ with parallel server extraction and stream verification.
 */

const cheerio = require("cheerio");
const crypto = require("crypto");
const https = require("https");
const http = require("http");
const zlib = require("zlib");

function nodeHttpsStreamAdapter(config) {
  return new Promise((resolve, reject) => {
    function executeRequest(currentUrl, redirectCount = 0) {
      if (redirectCount > 5) {
        return reject(new Error("Too many redirects"));
      }
      try {
        const u = new URL(currentUrl);
        const method = (config.method || "GET").toUpperCase();

        const flatHeaders = {};
        if (config.headers) {
          if (typeof config.headers.toJSON === "function") {
            Object.assign(flatHeaders, config.headers.toJSON());
          } else {
            Object.assign(flatHeaders, config.headers);
          }
        }

        let callerReferer = null;
        let callerOrigin = null;
        for (const k of Object.keys(flatHeaders)) {
          const lk = k.toLowerCase();
          if (lk === "referer") callerReferer = flatHeaders[k];
          if (lk === "origin") callerOrigin = flatHeaders[k];
          if (
            lk === "user-agent" ||
            lk === "referer" ||
            lk === "origin" ||
            lk === "host" ||
            lk === "accept-encoding"
          ) {
            delete flatHeaders[k];
          }
        }

        flatHeaders["Host"] = u.hostname;
        flatHeaders["User-Agent"] =
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

        const hostLower = u.hostname.toLowerCase();
        const pathLower = u.pathname.toLowerCase();

        // 1. Honor caller-supplied Referer & Origin (sanitize localhost Origins for as-cdn)
        if (hostLower.includes("as-cdn")) {
          flatHeaders["Referer"] =
            callerReferer && !callerReferer.includes("localhost")
              ? callerReferer
              : "https://ravok.buzz/";
          // as-cdn*.top returns HTTP 500 on localhost Origins; only send Origin if explicitly valid
          if (
            callerOrigin &&
            !callerOrigin.includes("localhost") &&
            !callerOrigin.includes("127.0.0.1")
          ) {
            flatHeaders["Origin"] = callerOrigin;
          }
        } else if (callerReferer) {
          flatHeaders["Referer"] = callerReferer;
          flatHeaders["Origin"] =
            callerOrigin || callerReferer.replace(/\/+$/, "");
        } else if (hostLower.includes("ravok.buzz")) {
          flatHeaders["Referer"] = "https://ravok.buzz/";
          flatHeaders["Origin"] = "https://ravok.buzz";
        } else if (
          hostLower.includes("playdrive") ||
          hostLower.includes("play4u") ||
          hostLower.includes("gothamnet") ||
          hostLower.includes("uprising") ||
          hostLower.includes("furiosamedia") ||
          pathLower.includes("/range/")
        ) {
          const baseHost =
            hostLower.includes("play4u") || hostLower.includes("uprising")
              ? "https://play4u.org/"
              : "https://playdrive.vip/";
          flatHeaders["Referer"] = baseHost;
          flatHeaders["Origin"] = baseHost.replace(/\/+$/, "");
        } else if (
          hostLower.includes("hdstream4u") ||
          hostLower.includes("acek-cdn") ||
          hostLower.includes("dramiyos")
        ) {
          flatHeaders["Referer"] = "https://hdstream4u.com/";
          flatHeaders["Origin"] = "https://hdstream4u.com";
        } else if (
          hostLower.includes("vidrift") ||
          hostLower.includes("hakunaymatata")
        ) {
          flatHeaders["Referer"] = "https://embed.vidrift.in/";
          flatHeaders["Origin"] = "https://embed.vidrift.in";
        } else if (hostLower.includes("vidly") || hostLower.includes("rousav")) {
          flatHeaders["Referer"] = "https://vidly.pro/";
          flatHeaders["Origin"] = "https://vidly.pro";
        } else if (hostLower.includes("vidbolt")) {
          flatHeaders["Referer"] = "https://vidbolt.xyz/";
          flatHeaders["Origin"] = "https://vidbolt.xyz";
        } else if (hostLower.includes("cinezo")) {
          flatHeaders["Referer"] = "https://player.cinezo.live/";
          flatHeaders["Origin"] = "https://player.cinezo.live";
        } else if (hostLower.includes("vidy")) {
          flatHeaders["Referer"] = "https://www.vidy.st/";
          flatHeaders["Origin"] = "https://www.vidy.st";
        } else if (hostLower.includes("lightspeed")) {
          flatHeaders["Referer"] = "https://cinejoy.pk/";
          flatHeaders["Origin"] = "https://cinejoy.pk";
        } else if (hostLower.includes("vidbasic")) {
          flatHeaders["Referer"] = "https://vidbasic.top/";
          flatHeaders["Origin"] = "https://vidbasic.top";
        } else if (
          hostLower.includes("vidmoly") ||
          hostLower.includes("vmnow") ||
          hostLower.includes("vmpx")
        ) {
          flatHeaders["Referer"] = "https://vidmoly.org/";
          flatHeaders["Origin"] = "https://vidmoly.org";
        } else if (hostLower.includes("mixdrop")) {
          flatHeaders["Referer"] = "https://mixdrop.ag/";
        } else if (
          hostLower.includes("streamtape") ||
          hostLower.includes("watchadsontape")
        ) {
          flatHeaders["Referer"] = "https://streamtape.com/";
        } else if (
          hostLower.includes("streamwish") ||
          hostLower.includes("flaswish") ||
          hostLower.includes("strwish")
        ) {
          flatHeaders["Referer"] = "https://streamwish.to/";
        } else if (hostLower.includes("sssrr.org")) {
          flatHeaders["Referer"] = "https://player.abyssplayer.com/";
          flatHeaders["Origin"] = "https://player.abyssplayer.com";
        } else if (hostLower.includes("abyss")) {
          flatHeaders["Referer"] = "https://animesalt.ro/";
          flatHeaders["Origin"] = "https://animesalt.ro";
        } else if (hostLower.includes("nekomux")) {
          flatHeaders["Referer"] = "https://player.nekomux.com/";
          flatHeaders["Origin"] = "https://player.nekomux.com";
        } else if (hostLower.includes("reanime")) {
          flatHeaders["Referer"] = "https://reanime.to/";
          flatHeaders["Origin"] = "https://reanime.to";
        } else if (hostLower.includes("1hanime")) {
          flatHeaders["Referer"] = "https://1hanime.com/";
          flatHeaders["Origin"] = "https://1hanime.com";
        } else if (hostLower.includes("bingr")) {
          flatHeaders["Referer"] = "https://bingr.one/";
          flatHeaders["Origin"] = "https://bingr.one";
        } else if (hostLower.includes("animesalt.cx")) {
          flatHeaders["Referer"] = "https://animesalt.cx/";
        } else {
          flatHeaders["Referer"] = "https://animesalt.ro/";
        }

        if (!flatHeaders["Accept"]) {
          flatHeaders["Accept"] = "*/*";
        }

        const reqOptions = {
          protocol: u.protocol,
          hostname: u.hostname,
          port: u.port || (u.protocol === "https:" ? 443 : 80),
          path: u.pathname + u.search,
          method: method,
          headers: flatHeaders,
        };

        const transport = u.protocol === "https:" ? https : http;
        const req = transport.request(reqOptions, (res) => {
          const statusCode = res.statusCode || 200;
          const resHeaders = res.headers || {};

          if (
            (statusCode === 301 ||
              statusCode === 302 ||
              statusCode === 303 ||
              statusCode === 307 ||
              statusCode === 308) &&
            resHeaders.location
          ) {
            res.resume();
            const nextUrl = new URL(resHeaders.location, currentUrl).toString();
            const nextHost = new URL(nextUrl).hostname.toLowerCase();
            if (
              nextHost.includes("google.com") ||
              nextHost.includes("recaptcha")
            ) {
              const err = new Error(
                `Request rejected by upstream redirecting to ${nextHost}`
              );
              err.status = 403;
              err.isAxiosError = true;
              return reject(err);
            }
            return executeRequest(nextUrl, redirectCount + 1);
          }

          function createResponse(data) {
            return {
              status: statusCode,
              statusText: res.statusMessage || "OK",
              headers: resHeaders,
              config,
              request: req,
              data,
            };
          }

          function handleValidateStatus(responseObj) {
            const isValid =
              typeof config.validateStatus === "function"
                ? config.validateStatus(statusCode)
                : statusCode >= 200 && statusCode < 300;
            if (!isValid) {
              const err = new Error(
                `Request failed with status code ${statusCode}`
              );
              err.config = config;
              err.request = req;
              err.response = responseObj;
              err.status = statusCode;
              err.isAxiosError = true;
              return reject(err);
            }
            resolve(responseObj);
          }

          if (config.responseType === "stream" || method === "HEAD") {
            const resp = createResponse(res);
            return handleValidateStatus(resp);
          }

          const chunks = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => {
            let buf = Buffer.concat(chunks);
            const enc = (resHeaders["content-encoding"] || "").toLowerCase();
            try {
              if (enc === "gzip") {
                buf = zlib.gunzipSync(buf);
              } else if (enc === "deflate") {
                buf = zlib.inflateSync(buf);
              } else if (enc === "br") {
                buf = zlib.brotliDecompressSync(buf);
              }
            } catch (_) {}

            if (
              config.responseType === "arraybuffer" ||
              config.responseType === "buffer"
            ) {
              return handleValidateStatus(createResponse(buf));
            }

            const text = buf.toString("utf8");
            let data = text;
            try {
              data = JSON.parse(text);
            } catch (_) {}

            handleValidateStatus(createResponse(data));
          });
        });

        if (config.timeout && config.timeout > 0) {
          req.setTimeout(config.timeout, () => {
            req.destroy(new Error(`timeout of ${config.timeout}ms exceeded`));
          });
        }

        req.on("error", (err) => {
          reject(err);
        });

        if (config.data) {
          const body =
            typeof config.data === "string"
              ? config.data
              : JSON.stringify(config.data);
          req.write(body);
        }

        req.end();
      } catch (e) {
        reject(e);
      }
    }

    executeRequest(config.url);
  });
}

function installStreamProxyInterceptor() {
  if (!global.axios || !global.axios.interceptors?.request) return;

  if (global.__strawverseStreamInterceptorId !== undefined) {
    try {
      global.axios.interceptors.request.eject(
        global.__strawverseStreamInterceptorId
      );
    } catch (_) {}
  }

  global.__strawverseStreamInterceptorId =
    global.axios.interceptors.request.use(
      (config) => {
        try {
          const urlStr = String(config.url || "").toLowerCase();
          const isMediaStream =
            config.responseType === "stream" ||
            urlStr.includes(".m3u8") ||
            urlStr.includes("/cdn/hls/") ||
            urlStr.includes("/hls/") ||
            urlStr.includes(".ts") ||
            urlStr.includes(".m4s") ||
            urlStr.includes(".mp4") ||
            urlStr.includes("#video.mp4") ||
            urlStr.includes("gothamnet") ||
            urlStr.includes("uprising") ||
            urlStr.includes("furiosamedia") ||
            urlStr.includes("/range/") ||
            urlStr.includes("playdrive") ||
            urlStr.includes("play4u") ||
            urlStr.includes("hdstream") ||
            urlStr.includes("streamwish") ||
            urlStr.includes("streamtape") ||
            urlStr.includes("mixdrop") ||
            urlStr.includes("filelions") ||
            urlStr.includes("vidbasic") ||
            urlStr.includes("vidmoly") ||
            urlStr.includes("sssrr.org") ||
            urlStr.includes("abyss") ||
            urlStr.includes("nekomux") ||
            urlStr.includes("flixcloud") ||
            urlStr.includes("rundowncdn") ||
            urlStr.includes("fetch9") ||
            urlStr.includes("r2.1hanime.com") ||
            urlStr.includes("htstreaming.com") ||
            urlStr.includes("nhplayer.com") ||
            urlStr.includes("vidrift") ||
            urlStr.includes("hakunaymatata") ||
            urlStr.includes("rousav") ||
            urlStr.includes("vidbolt") ||
            urlStr.includes("cinezo") ||
            urlStr.includes("vidy") ||
            urlStr.includes("lightspeed") ||
            urlStr.includes("vidly") ||
            urlStr.includes("ravok.buzz") ||
            urlStr.includes("as-cdn");

          const isTarget =
            isMediaStream ||
            urlStr.includes("animesalt.cx") ||
            urlStr.includes("animesalt.ro") ||
            urlStr.includes("abyssplayer.com") ||
            urlStr.includes("abysscdn.com") ||
            urlStr.includes("nekomux.com") ||
            urlStr.includes("ravok.buzz") ||
            urlStr.includes("as-cdn");

          if (isTarget) {
            config.strawverseDirectHttp = true;
            config._forceNativeBridge = false;

            if (isMediaStream) {
              config.adapter = nodeHttpsStreamAdapter;
            }

            if (urlStr.includes("sssrr.org")) {
              try {
                const u = new URL(config.url);
                if (config.headers?.set) {
                  config.headers.set("Host", u.hostname);
                  config.headers.set(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                  );
                  config.headers.set(
                    "Referer",
                    "https://player.abyssplayer.com/"
                  );
                  config.headers.set(
                    "Origin",
                    "https://player.abyssplayer.com"
                  );
                } else {
                  config.headers = config.headers || {};
                  config.headers["Host"] = u.hostname;
                  config.headers["User-Agent"] =
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
                  config.headers["Referer"] =
                    "https://player.abyssplayer.com/";
                  config.headers["Origin"] = "https://player.abyssplayer.com";
                }
              } catch (_) {}
            }
          }
        } catch (_) {}
        return config;
      },
      (err) => Promise.reject(err)
    );
}

try {
  installStreamProxyInterceptor();
} catch (_) {}

const cxBaseUrl = "https://animesalt.cx";
const roBaseUrl = "https://animesalt.ro";
const baseUrl = cxBaseUrl;

// Caches to map slugs to series vs movies on animesalt.cx and prevent cross-page duplicates
const slugTypeCache = new Map();
const searchSeenByQuery = new Map();
const recentSeenCache = new Map();

// Last fetched anime context for numeric episodeId fallback
let lastAnimeContext = {
  slug: "",
  episodes: [],
};

// Declarative header rules for AnimeSalt (.cx & .ro), Ravok/FireHLS CDN, NekoMux, and Abyss
const headerRules = [
  {
    match: ["animesalt.cx", "img.animesalt.cx"],
    headers: {
      Referer: "https://animesalt.cx/",
      Origin: "https://animesalt.cx",
      "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    },
  },
  {
    match: [
      "animesalt.ro",
      "animesalt",
      "nekomux",
      "play.nekomux",
      "abyss",
      "abysscdn",
      "abyssplayer",
    ],
    headers: {
      Referer: "https://animesalt.ro/",
      Origin: "https://animesalt.ro",
      "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    },
  },
  {
    match: ["ravok.buzz"],
    headers: {
      Referer: "https://ravok.buzz/",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    },
    strip: ["Origin"],
  },
  {
    match: ["as-cdn"],
    headers: {
      Referer: "https://ravok.buzz/",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    },
    strip: ["Origin"],
  },
];

const subrequestHeaders = [
  {
    match: [
      "nekomux",
      "play.nekomux",
      "abyss",
      "abysscdn",
      "abyssplayer",
      "sssrr.org",
    ],
    headers: {
      Referer: "https://player.nekomux.com/",
      Origin: "https://player.nekomux.com",
    },
  },
  {
    match: ["ravok.buzz", "as-cdn"],
    headers: {
      Referer: "https://ravok.buzz/",
    },
    strip: ["Origin"],
  },
];

const bypassDomains = [
  "animesalt.cx",
  "animesalt.ro",
  "ravok.buzz",
  "nekomux.com",
  "play.nekomux.com",
  "abysscdn.com",
  "abyssplayer.com",
];

// Resilient HTTP GET helper (avoids duplicate fetches on 404/HTTP errors)
async function httpGet(url, options = {}) {
  const timeoutMs = options.timeout || 10000;
  if (global.axios) {
    try {
      const resp = await global.axios.get(url, {
        ...options,
        timeout: timeoutMs,
        validateStatus: (s) => s < 500,
      });
      return resp;
    } catch (err) {
      if (err.response) {
        return {
          status: err.response.status,
          data: err.response.data,
          headers: err.response.headers || {},
        };
      }
      // Network/transport failure -> fall through to native fetch
    }
  }

  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    ...(options.headers || {}),
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { headers, signal: controller.signal });
    const text = await res.text();
    let data = text;
    try {
      data = JSON.parse(text);
    } catch (_) {}
    return { data, status: res.status, headers: res.headers };
  } finally {
    clearTimeout(timer);
  }
}

// Resilient HTTP POST helper
async function httpPost(url, formBody, options = {}) {
  const timeoutMs = options.timeout || 10000;
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    ...(options.headers || {}),
  };

  if (global.axios) {
    try {
      return await global.axios.post(url, formBody, {
        ...options,
        headers,
        timeout: timeoutMs,
      });
    } catch (err) {
      if (err.response) {
        return {
          status: err.response.status,
          data: err.response.data,
          headers: err.response.headers || {},
        };
      }
    }
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: formBody,
      signal: controller.signal,
    });
    const text = await res.text();
    let data = text;
    try {
      data = JSON.parse(text);
    } catch (_) {}
    return { data, status: res.status, headers: res.headers };
  } finally {
    clearTimeout(timer);
  }
}

function normalizePage(arg) {
  if (typeof arg === "number" && Number.isFinite(arg)) {
    return Math.max(1, Math.floor(arg));
  }
  if (typeof arg === "string") {
    const p = parseInt(arg.trim(), 10);
    return isNaN(p) || p < 1 ? 1 : p;
  }
  if (arg && typeof arg === "object") {
    if (arg.page !== undefined) return normalizePage(arg.page);
    if (arg.filters && typeof arg.filters === "object") {
      return normalizePage(arg.filters);
    }
  }
  return 1;
}

function decodeHtmlEntities(str) {
  if (!str) return "";
  return String(str)
    .replace(/&#038;/g, "&")
    .replace(/&#38;/g, "&")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function normalizeUrl(url, fallbackBase = cxBaseUrl) {
  if (!url) return null;
  const trimmed = String(url).trim();
  if (!trimmed || trimmed.startsWith("data:")) return null;
  if (trimmed.startsWith("//")) return `https:${trimmed}`;
  if (trimmed.startsWith("/")) return `${fallbackBase}${trimmed}`;
  return trimmed;
}

function extractImage($img, fallbackBase = cxBaseUrl) {
  if (!$img || $img.length === 0) return null;
  const candidates = [
    $img.attr("data-src"),
    $img.attr("data-lazy-src"),
    $img.attr("data-original"),
    $img.attr("src"),
  ];
  for (const cand of candidates) {
    const norm = normalizeUrl(cand, fallbackBase);
    if (norm) return norm;
  }
  return null;
}

function cleanSlug(str) {
  if (!str) return "";
  return String(str)
    .trim()
    .replace(
      /^https?:\/\/[^\/]+\/(?:anime|series|movies|movie|episode)\//i,
      ""
    )
    .replace(/^https?:\/\/[^\/]+\//i, "")
    .replace(/^\/?(?:anime|series|movies|movie|episode)\//i, "")
    .replace(/^\/+|\/+$/g, "");
}

/**
 * Parse pagination controls across both Torofilm (.cx) and AnimeStream (.ro).
 * @param {object} $ - Cheerio root
 * @param {number} currentPage
 * @returns {{ currentPage: number, hasNextPage: boolean, totalPages: number }}
 */
function parsePagination($, currentPage) {
  let totalPages = currentPage;
  let hasNextPage = false;

  const nextEl = $(
    "a.next.page-numbers, a.next, .pagination .next, .nav-links .next"
  );
  if (nextEl.length > 0) {
    hasNextPage = true;
  }

  $(
    "a.page-numbers, span.page-numbers, a.page-link, span.page-link, .nav-links a"
  ).each((_, el) => {
    const text = $(el).text().trim();
    const upper = text.toUpperCase();
    if (upper === "NEXT" || upper === "»" || upper.includes("NEXT")) {
      hasNextPage = true;
    }
    const href = $(el).attr("href") || "";
    if (href.includes(`/page/${currentPage + 1}/`)) {
      hasNextPage = true;
    }
    const num = parseInt(text, 10);
    if (!isNaN(num) && num > totalPages) {
      totalPages = num;
    }
  });

  return {
    currentPage,
    hasNextPage: hasNextPage || totalPages > currentPage,
    totalPages,
  };
}

/**
 * Parse cards from animesalt.cx (Torofilm theme).
 * @param {object} $ - Cheerio root
 * @returns {Array<object>}
 */
function parseCxCards($) {
  const results = [];
  $("article.post").each((_, el) => {
    if ($(el).hasClass("episodes")) return;

    const aTag = $(el).find("a.lnk-blk").first();
    const href =
      aTag.attr("href") || $(el).find("a[href]").first().attr("href") || "";
    const slug = cleanSlug(href);
    if (!slug) return;

    const isMovieHref = /\/movies?\//i.test(href);
    const watchText = $(el).find(".watch").text().trim();
    const isMovie = isMovieHref || /movie/i.test(watchText);
    slugTypeCache.set(slug, isMovie ? "movies" : "series");

    const title =
      $(el).find("h2.entry-title, .entry-title").first().text().trim() ||
      aTag.attr("title") ||
      slug;

    const image = extractImage($(el).find("img").first(), cxBaseUrl);

    results.push({
      id: slug,
      title,
      image,
      type: isMovie ? "Movie" : "TV",
      subDub: "Multi",
      _sourceSite: "cx",
    });
  });
  return results;
}

/**
 * Parse cards from animesalt.ro (AnimeStream theme).
 * @param {object} $ - Cheerio root
 * @returns {Array<object>}
 */
function parseRoCards($) {
  const results = [];
  $("article.bs").each((_, el) => {
    const aTag = $(el).find(".bsx a.tip, .bsx a").first();
    const href = aTag.attr("href") || "";
    const slug = cleanSlug(href);
    if (!slug) return;

    const title =
      aTag.attr("title") ||
      $(el).find(".tt h2").first().text().trim() ||
      $(el).find(".tt").first().text().trim() ||
      slug;

    const image = extractImage($(el).find("img").first(), roBaseUrl);
    const epText = $(el).find(".epx").text().trim();
    const epNum = parseFloat(epText.replace(/[^0-9.]/g, "")) || 1;
    const type = $(el).find(".typez").text().trim() || "TV";
    const subDub = $(el).find(".sb").text().trim() || "Dub";

    results.push({
      id: slug,
      title,
      episode: epNum,
      image,
      type,
      subDub,
      _sourceSite: "ro",
    });
  });
  return results;
}

/**
 * Filter & deduplicate merged results across pages so scrolling never duplicates items.
 */
function mergePaginatedCards(cxRes, roRes, pageNum, seenMap) {
  const merged = [];
  const batchSeen = new Set();

  for (const item of [...cxRes, ...roRes]) {
    const key = item.id;
    if (!key || batchSeen.has(key)) continue;

    const firstPage = seenMap.get(key);
    if (firstPage !== undefined && firstPage < pageNum) {
      continue;
    }

    batchSeen.add(key);
    if (firstPage === undefined) {
      seenMap.set(key, pageNum);
    }

    const { _sourceSite, ...cleanItem } = item;
    merged.push(cleanItem);
  }

  return merged;
}

/**
 * Search for anime across both animesalt.cx and animesalt.ro.
 * @param {string} query
 * @param {number|object} [page=1]
 * @returns {Promise<{ currentPage: number, hasNextPage: boolean, totalPages: number, results: Array<object> }>}
 */
async function SearchAnime(query, page = 1) {
  try {
    const trimmedQuery = String(query || "").trim();
    if (!trimmedQuery) {
      return await fetchRecentEpisodes(page);
    }

    const pageNum = normalizePage(page);
    const encoded = encodeURIComponent(trimmedQuery);
    const cxUrl =
      pageNum > 1
        ? `${cxBaseUrl}/page/${pageNum}/?s=${encoded}`
        : `${cxBaseUrl}/?s=${encoded}`;
    const roUrl =
      pageNum > 1
        ? `${roBaseUrl}/page/${pageNum}/?s=${encoded}`
        : `${roBaseUrl}/?s=${encoded}`;

    const [cxResp, roResp] = await Promise.allSettled([
      httpGet(cxUrl, { headers: { Referer: `${cxBaseUrl}/` }, timeout: 9000 }),
      httpGet(roUrl, { headers: { Referer: `${roBaseUrl}/` }, timeout: 9000 }),
    ]);

    let cxCards = [];
    let cxPag = { currentPage: pageNum, hasNextPage: false, totalPages: pageNum };
    if (
      cxResp.status === "fulfilled" &&
      cxResp.value &&
      cxResp.value.status < 400 &&
      typeof cxResp.value.data === "string"
    ) {
      const $cx = cheerio.load(cxResp.value.data);
      cxCards = parseCxCards($cx);
      cxPag = parsePagination($cx, pageNum);
    }

    let roCards = [];
    let roPag = { currentPage: pageNum, hasNextPage: false, totalPages: pageNum };
    if (
      roResp.status === "fulfilled" &&
      roResp.value &&
      roResp.value.status < 400 &&
      typeof roResp.value.data === "string"
    ) {
      const $ro = cheerio.load(roResp.value.data);
      roCards = parseRoCards($ro);
      roPag = parsePagination($ro, pageNum);
    }

    const qKey = trimmedQuery.toLowerCase();
    if (!searchSeenByQuery.has(qKey)) {
      if (searchSeenByQuery.size > 50) searchSeenByQuery.clear();
      searchSeenByQuery.set(qKey, new Map());
    }
    const seenMap = searchSeenByQuery.get(qKey);
    const results = mergePaginatedCards(cxCards, roCards, pageNum, seenMap);

    const totalPages = Math.max(
      cxCards.length > 0 ? cxPag.totalPages : 0,
      roCards.length > 0 ? roPag.totalPages : 0,
      results.length > 0 ? pageNum : 0
    );
    const hasNextPage =
      (cxCards.length > 0 && cxPag.hasNextPage) ||
      (roCards.length > 0 && roPag.hasNextPage);

    return {
      currentPage: pageNum,
      current_page: pageNum,
      page: pageNum,
      hasNextPage,
      totalPages,
      results,
    };
  } catch (err) {
    console.error("AnimeSalt SearchAnime error:", err.message);
    const pNum = normalizePage(page);
    return {
      currentPage: pNum,
      current_page: pNum,
      page: pNum,
      hasNextPage: false,
      totalPages: 0,
      results: [],
    };
  }
}

/**
 * Parse episode articles from Torofilm HTML (#episode_by_temp or AJAX response).
 */
function parseCxEpisodeElements($) {
  const list = [];
  $("article.episodes, #episode_by_temp li article").each((idx, el) => {
    const aTag = $(el).find("a.lnk-blk, a[href*='/episode/']").first();
    const href = aTag.attr("href") || "";
    const epSlug = cleanSlug(href);
    if (!epSlug) return;

    const numText = $(el).find(".num-epi").text().trim();
    const parsedNum = parseFloat(numText);
    const title =
      $(el).find("h2.entry-title, .entry-title").first().text().trim() ||
      aTag.attr("title") ||
      `Episode ${parsedNum || idx + 1}`;
    const thumbnail = extractImage($(el).find("img").first(), cxBaseUrl);

    list.push({
      id: epSlug,
      rawNumber: !isNaN(parsedNum) ? parsedNum : null,
      title,
      thumbnail,
    });
  });
  return list;
}

/**
 * Fetch and parse anime metadata and episodes from animesalt.cx (/series/ or /movies/).
 */
async function fetchCxInfo(slug) {
  const cachedType = slugTypeCache.get(slug);
  const pathsToTry =
    cachedType === "movies"
      ? [`${cxBaseUrl}/movies/${slug}/`, `${cxBaseUrl}/series/${slug}/`]
      : [`${cxBaseUrl}/series/${slug}/`, `${cxBaseUrl}/movies/${slug}/`];

  let html = "";
  let usedUrl = pathsToTry[0];

  for (const targetUrl of pathsToTry) {
    try {
      const res = await httpGet(targetUrl, {
        headers: { Referer: `${cxBaseUrl}/` },
        timeout: 9000,
      });
      if (
        res &&
        res.status < 400 &&
        typeof res.data === "string" &&
        res.data.includes("<h1")
      ) {
        html = res.data;
        usedUrl = targetUrl;
        break;
      }
    } catch (_) {}
  }

  if (!html) return null;

  const $ = cheerio.load(html);
  const isMoviePage = usedUrl.includes("/movies/");
  slugTypeCache.set(slug, isMoviePage ? "movies" : "series");

  const info = {
    id: slug,
    title: "",
    image: null,
    description: "",
    genres: [],
    languages: [],
    status: "Unknown",
    type: isMoviePage ? "Movie" : "TV",
    aired: "",
    episodes: [],
  };

  info.title = $("h1").first().text().trim() || slug;

  const posterImg = $(
    "img[data-src*='tmdb'], img[src*='tmdb'], img[data-src*='img.animesalt']"
  )
    .filter((_, el) => {
      const s =
        $(el).attr("data-src") || $(el).attr("src") || "";
      return !s.includes("AnimeSaltLong") && !s.includes("thumb_");
    })
    .first();
  info.image = extractImage(posterImg, cxBaseUrl);

  info.description =
    $("#overview-text").text().trim() ||
    $(".description").first().text().trim() ||
    "";

  $("a[href*='/category/genre/']").each((_, el) => {
    const g = $(el).text().trim();
    if (g && !info.genres.includes(g)) info.genres.push(g);
  });

  $("a[href*='/category/language/']").each((_, el) => {
    const l = $(el).text().trim();
    if (l && !info.languages.includes(l)) info.languages.push(l);
  });

  // Extract year if shown in pill badges
  const metaText = $(".bd > div").slice(0, 6).text().replace(/\s+/g, " ");
  const yearMatch = metaText.match(/\b(19\d{2}|20\d{2})\b/);
  if (yearMatch) info.aired = yearMatch[1];

  if (isMoviePage) {
    info.episodes.push({
      id: slug,
      number: 1,
      title: info.title || "Full Movie",
      thumbnail: info.image,
    });
    return info;
  }

  // Extract Season 1 from initial page, and fetch remaining seasons via AJAX GET
  const seasonBtns = $("a.season-btn");
  if (seasonBtns.length > 1) {
    const seasonJobs = [];
    seasonBtns.each((idx, btn) => {
      const sNum = $(btn).attr("data-season");
      const postId = $(btn).attr("data-post");
      const isActive = $(btn).hasClass("active") || idx === 0;
      if (isActive) {
        seasonJobs.push(Promise.resolve(parseCxEpisodeElements($)));
      } else if (sNum && postId) {
        const ajaxUrl = `${cxBaseUrl}/wp-admin/admin-ajax.php?action=action_select_season&season=${encodeURIComponent(
          sNum
        )}&post=${encodeURIComponent(postId)}`;
        seasonJobs.push(
          httpGet(ajaxUrl, {
            headers: { Referer: usedUrl },
            timeout: 9000,
          })
            .then((r) =>
              r && r.status < 400 && typeof r.data === "string"
                ? parseCxEpisodeElements(cheerio.load(r.data))
                : []
            )
            .catch(() => [])
        );
      }
    });

    const allSeasons = await Promise.all(seasonJobs);
    const seenEpIds = new Set();
    let maxEpNum = 0;

    for (const seasonList of allSeasons) {
      let resetOffset = 0;
      if (
        seasonList.length > 0 &&
        seasonList[0].rawNumber !== null &&
        seasonList[0].rawNumber <= maxEpNum
      ) {
        resetOffset = maxEpNum;
      }

      seasonList.forEach((ep, idx) => {
        if (seenEpIds.has(ep.id)) return;
        seenEpIds.add(ep.id);

        const epNumber =
          resetOffset > 0
            ? resetOffset + idx + 1
            : ep.rawNumber !== null
              ? ep.rawNumber
              : maxEpNum + 1;
        if (epNumber > maxEpNum) maxEpNum = epNumber;

        info.episodes.push({
          id: ep.id,
          number: epNumber,
          title: ep.title,
          thumbnail: ep.thumbnail,
        });
      });
    }
  } else {
    const parsedEps = parseCxEpisodeElements($);
    const seenEpIds = new Set();
    parsedEps.forEach((ep, idx) => {
      if (seenEpIds.has(ep.id)) return;
      seenEpIds.add(ep.id);
      info.episodes.push({
        id: ep.id,
        number: ep.rawNumber !== null ? ep.rawNumber : idx + 1,
        title: ep.title,
        thumbnail: ep.thumbnail,
      });
    });
  }

  return info;
}

/**
 * Fetch and parse anime metadata and episodes from animesalt.ro (/anime/).
 */
async function fetchRoInfo(slug) {
  try {
    const url = `${roBaseUrl}/anime/${slug}/`;
    const res = await httpGet(url, {
      headers: { Referer: `${roBaseUrl}/` },
      timeout: 9000,
    });
    if (!res || res.status >= 400 || typeof res.data !== "string") return null;
    const html = res.data;
    if (!html) return null;

    const $ = cheerio.load(html);
    const title =
      $("h1.entry-title").text().trim() ||
      $('h1[itemprop="headline"]').text().trim() ||
      $('h1[itemprop="name"]').text().trim() ||
      $(".infox h1").text().trim() ||
      "";
    if (!title) return null;

    const info = {
      id: slug,
      title,
      image: extractImage(
        $(".thumb img.wp-post-image, .thumb img, .poster img").first(),
        roBaseUrl
      ),
      description:
        $(".entry-content[itemprop='description']").text().trim() ||
        $(".entry-content").first().text().trim() ||
        $(".synopsis").text().trim() ||
        "",
      genres: [],
      languages: [],
      status: "Unknown",
      type: "TV",
      aired: "",
      episodes: [],
    };

    $(".genxed:not(.langxed) a, .spe span:contains('Genre') a").each(
      (_, el) => {
        const g = $(el).text().trim();
        if (g && !info.genres.includes(g)) info.genres.push(g);
      }
    );

    const statusEl = $("span:contains('Status:')")
      .first()
      .text()
      .replace(/Status:/i, "")
      .trim();
    if (
      statusEl.toLowerCase().includes("completed") ||
      statusEl.toLowerCase().includes("ended")
    ) {
      info.status = "Completed";
    } else if (
      statusEl.toLowerCase().includes("ongoing") ||
      statusEl.toLowerCase().includes("returning")
    ) {
      info.status = "Ongoing";
    } else if (statusEl) {
      info.status = statusEl;
    }

    info.aired =
      $("span:contains('Released:')")
        .first()
        .text()
        .replace(/Released:/i, "")
        .trim() ||
      $("span:contains('Aired:')")
        .first()
        .text()
        .replace(/Aired:/i, "")
        .trim() ||
      "";

    $(".genxed.langxed a, .langxed a").each((_, el) => {
      const l = $(el).text().trim();
      if (l && !info.languages.includes(l)) info.languages.push(l);
    });

    const typeText =
      $(".typez").first().text().trim() ||
      $(".spe span:contains('Type')").text().trim();
    if (typeText) {
      info.type = typeText.includes("Movie") ? "Movie" : "TV";
    }

    $("a.asc-epcard").each((idx, el) => {
      const epSlug = cleanSlug($(el).attr("href") || "");
      if (!epSlug) return;
      const epNum = parseFloat($(el).find(".asc-epnum").text().trim()) || idx + 1;
      const epTitle =
        $(el).find(".asc-eptitle").attr("title") ||
        $(el).find(".asc-eptitle").text().trim() ||
        `Episode ${epNum}`;
      const thumbnail = extractImage(
        $(el).find(".asc-epthumb img").first(),
        roBaseUrl
      );
      info.episodes.push({ id: epSlug, number: epNum, title: epTitle, thumbnail });
    });

    if (info.episodes.length === 0) {
      $(".eplister li a, .episodelist ul li a").each((idx, el) => {
        const epSlug = cleanSlug($(el).attr("href") || "");
        if (!epSlug) return;
        const epNum =
          parseFloat($(el).find(".epl-num").text().trim()) || idx + 1;
        const epTitle =
          $(el).find(".epl-title").text().trim() || `Episode ${epNum}`;
        info.episodes.push({ id: epSlug, number: epNum, title: epTitle });
      });
    }

    return info;
  } catch (_) {
    return null;
  }
}

/**
 * Fetch detailed anime metadata and complete episode list combining animesalt.cx & animesalt.ro.
 * @param {string} id - Anime slug or URL
 */
async function AnimeInfo(id) {
  const slug = cleanSlug(id);
  const animeInfo = {
    id: slug,
    title: "",
    image: null,
    image_url: null,
    description: "",
    genres: [],
    status: "Unknown",
    type: "TV",
    aired: "",
    languages: [],
    totalEpisodes: 0,
    episodes: [],
  };

  try {
    const [cxRes, roRes] = await Promise.allSettled([
      fetchCxInfo(slug),
      fetchRoInfo(slug),
    ]);

    const cx = cxRes.status === "fulfilled" ? cxRes.value : null;
    const ro = roRes.status === "fulfilled" ? roRes.value : null;

    const primary = cx || ro;
    const secondary = cx ? ro : null;

    if (primary) {
      animeInfo.title = primary.title || secondary?.title || slug;
      animeInfo.image = primary.image || secondary?.image || null;
      animeInfo.image_url = animeInfo.image;
      animeInfo.description =
        primary.description || secondary?.description || "";
      animeInfo.genres =
        primary.genres.length > 0 ? primary.genres : secondary?.genres || [];
      animeInfo.languages =
        primary.languages.length > 0
          ? primary.languages
          : secondary?.languages || [];
      animeInfo.status =
        primary.status && primary.status !== "Unknown"
          ? primary.status
          : secondary?.status || "Unknown";
      animeInfo.type = primary.type || secondary?.type || "TV";
      animeInfo.aired = primary.aired || secondary?.aired || "";

      const baseEpisodes =
        primary.episodes.length > 0 ? [...primary.episodes] : [];
      const seenEp = new Set(baseEpisodes.map((e) => cleanSlug(e.id)));

      if (secondary && Array.isArray(secondary.episodes)) {
        for (const ep of secondary.episodes) {
          const k = cleanSlug(ep.id);
          if (!seenEp.has(k) && !baseEpisodes.some((b) => b.number === ep.number)) {
            seenEp.add(k);
            baseEpisodes.push(ep);
          }
        }
      }

      if (baseEpisodes.length === 0) {
        baseEpisodes.push({
          id: slug,
          number: 1,
          title: animeInfo.title || "Full Movie",
          thumbnail: animeInfo.image,
        });
      }

      animeInfo.episodes = baseEpisodes;
      animeInfo.totalEpisodes = baseEpisodes.length;
    }

    lastAnimeContext = {
      slug,
      episodes: animeInfo.episodes,
    };
    return animeInfo;
  } catch (err) {
    console.error("AnimeSalt AnimeInfo error:", err.message);
    return animeInfo;
  }
}

/**
 * Fetch episode list for StrawVerse navigation.
 */
async function fetchEpisode(id, page = 1) {
  try {
    const info = await AnimeInfo(id);
    return {
      episodes: info.episodes || [],
      totalPages: 1,
      total: (info.episodes || []).length,
      currentPage: 1,
    };
  } catch (err) {
    console.error("AnimeSalt fetchEpisode error:", err.message);
    return { episodes: [], totalPages: 0, total: 0, currentPage: page };
  }
}

/**
 * Decrypt CryptoJSAesJson payload (EVP_BytesToKey MD5 OpenSSL key/iv) used by FireHLS when hls=false.
 */
function decryptCryptoJsAesJson(jsonStr, passphraseRaw) {
  try {
    const parsed =
      typeof jsonStr === "string" ? JSON.parse(jsonStr) : jsonStr;
    if (!parsed || !parsed.ct || !parsed.s) return null;

    // Unescape \xNN sequences if present in ck
    const passphrase = String(passphraseRaw || "").replace(
      /\\x([0-9A-Fa-f]{2})/g,
      (_, hex) => String.fromCharCode(parseInt(hex, 16))
    );

    const salt = Buffer.from(parsed.s, "hex");
    const ct = Buffer.from(parsed.ct, "base64");
    const iv = parsed.iv ? Buffer.from(parsed.iv, "hex") : null;

    // Derive 32-byte key + 16-byte IV using OpenSSL EVP_BytesToKey (MD5)
    const passBuf = Buffer.from(passphrase, "utf8");
    const hashes = [];
    let prev = Buffer.alloc(0);
    let totalLen = 0;
    while (totalLen < 48) {
      const md5 = crypto.createHash("md5");
      md5.update(prev);
      md5.update(passBuf);
      md5.update(salt);
      prev = md5.digest();
      hashes.push(prev);
      totalLen += prev.length;
    }
    const derived = Buffer.concat(hashes);
    const key = derived.subarray(0, 32);
    const finalIv = iv || derived.subarray(32, 48);

    const decipher = crypto.createDecipheriv("aes-256-cbc", key, finalIv);
    let decrypted = decipher.update(ct);
    decrypted = Buffer.concat([decrypted, decipher.final()]);
    const resultStr = decrypted.toString("utf8");
    try {
      return JSON.parse(resultStr);
    } catch (_) {
      return resultStr;
    }
  } catch (_) {
    return null;
  }
}

/**
 * Unpack Packer eval block from FireHLS / ravok.buzz embed HTML.
 */
function unpackPackerEval(html) {
  try {
    const m = html.match(
      /eval\((function\(p,a,c,k,e,d\)[\s\S]+?\.split\([^\)]+\),0,\{\}\))\)/
    );
    if (!m) return "";
    // Extract arguments (p, a, c, k) safely without executing arbitrary code
    const fnCall = m[1];
    const argsMatch = fnCall.match(
      /return p\}\('((?:\\.|[^'])*)',(\d+),(\d+),'((?:\\.|[^'])*)'\.split\('\|'\)/
    );
    if (!argsMatch) return "";
    let p = argsMatch[1].replace(/\\'/g, "'").replace(/\\\\/g, "\\");
    const a = parseInt(argsMatch[2], 10);
    let c = parseInt(argsMatch[3], 10);
    const k = argsMatch[4].split("|");
    const encode = (num) =>
      (num < a ? "" : encode(Math.floor(num / a))) +
      ((num % a) > 35
        ? String.fromCharCode((num % a) + 29)
        : (num % a).toString(36));
    const dict = {};
    while (c--) {
      dict[encode(c)] = k[c] || encode(c);
    }
    return p.replace(/\b\w+\b/g, (word) => dict[word] || word);
  } catch (_) {
    return "";
  }
}

/**
 * Probe a subtitle track via 512-byte Range GET to detect VTT vs SRT,
 * skip ASS vector-drawing junk, and append /sub.srt or /sub.vtt when accepted.
 */
async function probeAndNormalizeSubtitle(rawSub, refererOrigin) {
  try {
    const rawUrl = normalizeUrl(rawSub.url, refererOrigin);
    if (!rawUrl || !rawUrl.startsWith("http")) return null;

    const res = await httpGet(rawUrl, {
      headers: {
        Referer: `${refererOrigin}/`,
        Range: "bytes=0-600",
      },
      timeout: 4500,
    });

    if (!res || res.status >= 400 || typeof res.data !== "string") {
      return null;
    }

    const head = res.data.trim();
    if (!head || /<html|<!DOCTYPE/i.test(head)) return null;

    // Filter out converted ASS vector-drawing coordinate tracks (e.g. "m -111 -68.5 l ...")
    if (/^\d+\s+[\d:,.]+\s*-->[^\n]*\n\s*m\s+-?\d+(?:\.\d+)?\s+-?\d+/im.test(head)) {
      return null;
    }

    const isVtt = /^WEBVTT\b/i.test(head);
    const ext = isVtt ? "vtt" : "srt";
    let finalUrl = rawUrl;

    // Rewrite masked .jpg/.png/.webp extension if CDN accepts /sub.srt or /sub.vtt
    // so Android ExoPlayer & StrawVerse Downloader detect the exact subtitle format.
    if (/\.(jpg|jpeg|png|webp)(\?.*)?$/i.test(rawUrl) && !rawUrl.includes("?")) {
      const candidateExtUrl = `${rawUrl}/sub.${ext}`;
      try {
        const extProbe = await httpGet(candidateExtUrl, {
          headers: { Referer: `${refererOrigin}/`, Range: "bytes=0-64" },
          timeout: 3500,
        });
        if (extProbe && extProbe.status < 400) {
          finalUrl = candidateExtUrl;
        }
      } catch (_) {}
    }

    let label = rawSub.lang || "English";
    if (isVtt && /\[(?:footsteps|music|panting|groans|sighs|door)/i.test(head)) {
      if (!label.toLowerCase().includes("sdh")) label = `${label} (SDH)`;
    }

    return {
      url: finalUrl,
      lang: label,
      isVtt,
      isDialogue: !isVtt && !label.toLowerCase().includes("sign"),
    };
  } catch (_) {
    return null;
  }
}

/**
 * Resolve FireHLS / VPlayer embed (ravok.buzz/video/<hash> or as-cdn26.top/video/<hash>).
 */
async function resolveFireHlsStream(embedUrl, originSite, labelHint = "") {
  const result = { sources: [], subtitles: [] };
  const hashMatch = embedUrl.match(/\/video\/([a-zA-Z0-9_-]+)/);
  if (!hashMatch) return result;
  const videoId = hashMatch[1];

  const candidates = [embedUrl];
  try {
    const u = new URL(embedUrl);
    if (!u.hostname.includes("ravok.buzz")) {
      candidates.push(`https://ravok.buzz/video/${videoId}`);
    }
  } catch (_) {}

  for (const targetEmbed of candidates) {
    try {
      const playerOrigin = new URL(targetEmbed).origin;
      const apiUrl = `${playerOrigin}/player/index.php?data=${videoId}&do=getVideo`;
      const formBody = `hash=${encodeURIComponent(videoId)}&r=${encodeURIComponent(
        `${originSite}/`
      )}`;

      const [pageResp, apiResp] = await Promise.allSettled([
        httpGet(targetEmbed, {
          headers: { Referer: `${originSite}/` },
          timeout: 8000,
        }),
        httpPost(apiUrl, formBody, {
          headers: {
            Referer: targetEmbed,
            Origin: playerOrigin,
          },
          timeout: 8000,
        }),
      ]);

      const apiData =
        apiResp.status === "fulfilled" ? apiResp.value?.data : null;
      if (!apiData || typeof apiData !== "object") {
        continue;
      }

      if (typeof global.registerStreamHeaders === "function") {
        global.registerStreamHeaders(new URL(playerOrigin).hostname, {
          Referer: `${playerOrigin}/`,
        });
      }

      const resolvedMedia = [];
      if (apiData.hls && (apiData.videoSource || apiData.securedLink)) {
        resolvedMedia.push({
          url: apiData.videoSource || apiData.securedLink,
          isM3U8: true,
          quality: "1080p (Multi-Audio HLS)",
        });
      } else if (!apiData.hls && Array.isArray(apiData.videoSources)) {
        for (const vs of apiData.videoSources) {
          const decUrl = decryptCryptoJsAesJson(vs.file, apiData.ck);
          if (typeof decUrl === "string" && decUrl.startsWith("http")) {
            resolvedMedia.push({
              url: decUrl,
              isM3U8: decUrl.includes(".m3u8"),
              quality: vs.label || "HD",
            });
          }
        }
      }

      if (resolvedMedia.length === 0) continue;

      for (const m of resolvedMedia) {
        result.sources.push({
          name: labelHint
            ? `AnimeSalt ${labelHint} (Multi-Audio)`
            : "AnimeSalt playX (Multi-Audio: Hindi / Eng / Jap / Tam / Tel)",
          quality: m.quality,
          url: m.url,
          isM3U8: m.isM3U8,
          headers: {
            Referer: `${playerOrigin}/`,
          },
          _providerTag: "firehls",
        });
      }

      // Extract subtitles from embed HTML (playerjsSubtitle + unpacked VPlayer tracks)
      const htmlStr =
        pageResp.status === "fulfilled" &&
        typeof pageResp.value?.data === "string"
          ? pageResp.value.data
          : "";

      if (htmlStr) {
        const rawSubs = [];
        const pjsMatch = htmlStr.match(
          /var\s+playerjsSubtitle\s*=\s*"([^"]+)"/
        );
        if (pjsMatch && pjsMatch[1]) {
          for (const part of pjsMatch[1].split(",")) {
            const m = part.trim().match(/^\[([^\]]+)\](https?:\/\/.+)$/);
            if (m) rawSubs.push({ lang: m[1].trim(), url: m[2].trim() });
          }
        }

        if (rawSubs.length === 0) {
          const unpacked = unpackPackerEval(htmlStr);
          if (unpacked) {
            const trackMatches = unpacked.matchAll(
              /"kind"\s*:\s*"captions"[^}]*"file"\s*:\s*"([^"]+)"[^}]*"label"\s*:\s*"([^"]+)"/g
            );
            for (const tm of trackMatches) {
              rawSubs.push({
                lang: tm[2],
                url: tm[1].replace(/\\\//g, "/"),
              });
            }
          }
        }

        if (rawSubs.length > 0) {
          const probed = await Promise.all(
            rawSubs
              .slice(0, 8)
              .map((s) => probeAndNormalizeSubtitle(s, playerOrigin))
          );
          const validSubs = probed.filter(Boolean);
          // Sort English dialogue tracks first, then other dialogue tracks, then SDH/VTT
          validSubs.sort((a, b) => {
            if (a.isDialogue !== b.isDialogue) return a.isDialogue ? -1 : 1;
            const aEng = String(a.lang || "").toLowerCase().includes("english");
            const bEng = String(b.lang || "").toLowerCase().includes("english");
            if (aEng !== bEng) return aEng ? -1 : 1;
            return 0;
          });

          const usedLabels = new Map();
          for (const sub of validSubs) {
            const count = (usedLabels.get(sub.lang) || 0) + 1;
            usedLabels.set(sub.lang, count);
            const finalLang = count > 1 ? `${sub.lang} ${count}` : sub.lang;
            result.subtitles.push({
              url: sub.url,
              lang: finalLang,
            });
          }
        }
      }

      return result;
    } catch (_) {}
  }

  return result;
}

/**
 * Resolve NekoMux player embed into master HLS stream.
 */
async function resolveNekomuxStream(embedUrl) {
  try {
    const match = embedUrl.match(/episode\/(\d+)/);
    if (!match) return null;
    const epNum = match[1];

    const sourceApi = `https://player.nekomux.com/episode/${epNum}/source`;
    const res = await httpGet(sourceApi, {
      headers: {
        Referer: `https://player.nekomux.com/episode/${epNum}`,
        "X-Requested-With": "XMLHttpRequest",
      },
      timeout: 8000,
    });

    const data = res?.data;
    if (data && data.ok && data.url) {
      return data.url;
    }
  } catch (err) {
    console.warn("NekoMux stream resolution notice:", err.message);
  }
  return null;
}

/**
 * Verify whether a resolved stream URL actually serves valid HLS/media bytes.
 * Returns { status: 'verified' | 'dead' | 'uncertain', reason?: string }
 */
async function verifyStreamSource(src) {
  if (!src || !src.url || !src.url.startsWith("http")) {
    return { status: "dead", reason: "Missing stream URL" };
  }

  const reqHeaders = { ...(src.headers || {}) };
  if (!src.isM3U8) {
    reqHeaders["Range"] = "bytes=0-1024";
  }

  try {
    const res = await httpGet(src.url, {
      headers: reqHeaders,
      timeout: 5000,
    });

    if (!res) {
      return { status: "uncertain", reason: "Empty response" };
    }
    if (res.status === 404 || res.status === 410 || res.status >= 500) {
      return { status: "dead", reason: `HTTP ${res.status}` };
    }
    if (res.status === 403 || res.status === 503) {
      return { status: "uncertain", reason: `Protected HTTP ${res.status}` };
    }
    if (res.status >= 400) {
      return { status: "dead", reason: `HTTP ${res.status}` };
    }

    const dataStr = typeof res.data === "string" ? res.data : "";
    const contentType = String(
      res.headers?.["content-type"] || res.headers?.get?.("content-type") || ""
    ).toLowerCase();

    if (
      contentType.includes("text/html") ||
      /<html|<!DOCTYPE/i.test(dataStr)
    ) {
      return { status: "dead", reason: "Returned HTML instead of media" };
    }

    if (src.isM3U8) {
      if (!dataStr.includes("#EXTM3U")) {
        return { status: "dead", reason: "Invalid M3U8 manifest" };
      }

      // Also check first variant playlist URI if master playlist so broken CDN shards are caught
      const lines = dataStr
        .split("\n")
        .map((l) => l.trim())
        .filter(Boolean);
      let childUri = null;
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].startsWith("#EXT-X-STREAM-INF:") && i + 1 < lines.length) {
          if (!lines[i + 1].startsWith("#")) {
            childUri = new URL(lines[i + 1], src.url).href;
            break;
          }
        }
      }

      if (childUri) {
        const subRes = await httpGet(childUri, {
          headers: src.headers || {},
          timeout: 5000,
        });
        if (!subRes || subRes.status === 404 || subRes.status >= 500) {
          return {
            status: "dead",
            reason: `Variant playlist HTTP ${subRes?.status || "error"}`,
          };
        }
        const subData = typeof subRes.data === "string" ? subRes.data : "";
        if (!subData.includes("#EXTM3U")) {
          return { status: "dead", reason: "Variant playlist invalid" };
        }
      }
    }

    return { status: "verified" };
  } catch (err) {
    const msg = String(err.message || "");
    if (msg.includes("ENOTFOUND") || msg.includes("EAI_AGAIN")) {
      return { status: "dead", reason: msg };
    }
    return { status: "uncertain", reason: msg };
  }
}

function resolveCandidateSlugs(id) {
  const raw = String(id || "").trim();
  const list = [];
  if (raw.includes(":")) {
    const parts = raw.split(":");
    const aSlug = cleanSlug(parts[0]);
    const ep = parseInt(parts[1], 10) || 1;
    list.push(`${aSlug}-1x${ep}`);
    list.push(`${aSlug}-${ep}`);
    list.push(`${aSlug}-episode-${ep}`);
    list.push(aSlug);
  } else if (/^\d+$/.test(raw)) {
    const epNum = parseInt(raw, 10);
    if (lastAnimeContext.episodes && lastAnimeContext.episodes.length > 0) {
      const match = lastAnimeContext.episodes.find(
        (e) => Number(e.number) === epNum
      );
      if (match && match.id) list.push(cleanSlug(match.id));
    }
    if (lastAnimeContext.slug) {
      list.push(`${lastAnimeContext.slug}-1x${epNum}`);
      list.push(`${lastAnimeContext.slug}-${epNum}`);
      list.push(`${lastAnimeContext.slug}-episode-${epNum}`);
    }
  } else {
    const clean = cleanSlug(raw);
    list.push(clean);
    const m1 = clean.match(/^(.+)-(\d+)x(\d+)$/);
    if (m1) {
      list.push(`${m1[1]}-${m1[3]}`);
      list.push(`${m1[1]}-episode-${m1[3]}`);
    }
    const m2 = clean.match(/^(.+)-episode-(\d+)$/i);
    if (m2) {
      list.push(`${m2[1]}-1x${m2[2]}`);
      list.push(`${m2[1]}-${m2[2]}`);
    }
    const m3 = clean.match(/^(.+)-(\d+)$/);
    if (m3) {
      list.push(`${m3[1]}-1x${m3[2]}`);
      list.push(`${m3[1]}-episode-${m3[2]}`);
    }
  }
  return [...new Set(list.filter(Boolean))];
}

/**
 * Fetch episode/movie HTML and extract server embeds from animesalt.cx.
 */
async function collectCxServers(candidateSlugs) {
  const primarySlug = candidateSlugs[0];
  const isMovieHint =
    slugTypeCache.get(primarySlug) === "movies" ||
    !/-\d+x\d+$/i.test(primarySlug);

  const urls = [];
  for (const s of candidateSlugs.slice(0, 2)) {
    if (isMovieHint && s === primarySlug) {
      urls.push(`${cxBaseUrl}/movies/${s}/`, `${cxBaseUrl}/episode/${s}/`);
    } else {
      urls.push(`${cxBaseUrl}/episode/${s}/`);
    }
  }
  if (!isMovieHint) {
    urls.push(`${cxBaseUrl}/movies/${primarySlug}/`);
  }

  for (const targetUrl of [...new Set(urls)]) {
    try {
      const res = await httpGet(targetUrl, {
        headers: { Referer: `${cxBaseUrl}/` },
        timeout: 8000,
      });
      if (!res || res.status >= 400 || typeof res.data !== "string") continue;
      const html = res.data;
      if (
        !html.includes("iframe") &&
        !html.includes("options-0") &&
        !html.includes("server-btn")
      ) {
        continue;
      }

      const $ = cheerio.load(html);
      const servers = [];
      const seenUrls = new Set();

      $(".server-btn").each((idx, el) => {
        const srvName = $(el).find(".server-info").text().trim();
        const title = srvName || $(el).text().trim() || `Server ${idx + 1}`;
        const iframe = $(`#options-${idx} iframe`).first();
        const url = decodeHtmlEntities(
          iframe.attr("src") || iframe.attr("data-src") || ""
        ).trim();
        if (url && !seenUrls.has(url)) {
          seenUrls.add(url);
          servers.push({ title, url, originSite: cxBaseUrl });
        }
      });

      $("div[id^='options-'] iframe").each((idx, el) => {
        const url = decodeHtmlEntities(
          $(el).attr("src") || $(el).attr("data-src") || ""
        ).trim();
        if (url && !seenUrls.has(url)) {
          seenUrls.add(url);
          servers.push({
            title: `Server ${idx + 1}`,
            url,
            originSite: cxBaseUrl,
          });
        }
      });

      if (servers.length > 0) {
        return servers;
      }
    } catch (_) {}
  }

  return [];
}

/**
 * Fetch episode HTML and extract server embeds from animesalt.ro.
 */
async function collectRoServers(candidateSlugs) {
  for (const slug of candidateSlugs.slice(0, 3)) {
    try {
      const res = await httpGet(`${roBaseUrl}/${slug}/`, {
        headers: { Referer: `${roBaseUrl}/` },
        timeout: 8000,
      });
      if (!res || res.status >= 400 || typeof res.data !== "string") continue;
      const dataStr = res.data;
      if (!dataStr.includes("server-button") && !dataStr.includes("loadMi")) {
        continue;
      }

      const $ = cheerio.load(dataStr);
      const servers = [];
      const seenUrls = new Set();

      $("button.server-button").each((_, el) => {
        const title =
          $(el).find(".server-button-title").text().trim() || "Server";
        const onclick = $(el).attr("onclick") || "";
        const b64Match = onclick.match(/value:\s*['"]([A-Za-z0-9+/=]+)['"]/);
        if (!b64Match) return;
        try {
          const iframeHtml = decodeHtmlEntities(
            Buffer.from(b64Match[1], "base64").toString("latin1")
          );
          const urlMatch =
            iframeHtml.match(/[?&]url=([^"&\s]+)/i) ||
            iframeHtml.match(/src="([^"]+)"/i);
          const targetUrl = urlMatch ? decodeURIComponent(urlMatch[1]) : "";
          if (targetUrl && !seenUrls.has(targetUrl)) {
            seenUrls.add(targetUrl);
            servers.push({ title, url: targetUrl, originSite: roBaseUrl });
          }
        } catch (_) {}
      });

      if (servers.length > 0) {
        return servers;
      }
    } catch (_) {}
  }

  return [];
}

/**
 * Fetch video sources and subtitles across both animesalt.cx and animesalt.ro,
 * resolve all candidates in parallel, verify reachability, and return working sources.
 */
async function fetchEpisodeSources(episodeId, category = null) {
  try {
    installStreamProxyInterceptor();

    try {
      if (typeof global.setDynamicReferer === "function") {
        global.setDynamicReferer("sssrr.org", "https://player.abyssplayer.com/");
        global.setDynamicReferer("abyssplayer.com", "https://animesalt.ro/");
        global.setDynamicReferer(
          "player.abyssplayer.com",
          "https://animesalt.ro/"
        );
        global.setDynamicReferer(
          "player.nekomux.com",
          "https://animesalt.ro/"
        );
        global.setDynamicReferer("nekomux.com", "https://animesalt.ro/");
        global.setDynamicReferer("ravok.buzz", "https://ravok.buzz/");
      }
    } catch (_) {}

    if (typeof global.registerStreamHeaders === "function") {
      global.registerStreamHeaders("player.nekomux.com", {
        Referer: "https://player.nekomux.com/",
        Origin: "https://player.nekomux.com",
      });
      global.registerStreamHeaders("play.nekomux.com", {
        Referer: "https://player.nekomux.com/",
        Origin: "https://player.nekomux.com",
      });
      global.registerStreamHeaders("ravok.buzz", {
        Referer: "https://ravok.buzz/",
      });
      global.registerStreamHeaders("animesalt.cx", {
        Referer: "https://animesalt.cx/",
      });
      global.registerStreamHeaders("animesalt.ro", {
        Referer: "https://animesalt.ro/",
      });
    }

    if (typeof global.registerSubrequestHeaders === "function") {
      global.registerSubrequestHeaders("play.nekomux.com", {
        Referer: "https://player.nekomux.com/",
        Origin: "https://player.nekomux.com",
      });
      global.registerSubrequestHeaders("ravok.buzz", {
        Referer: "https://ravok.buzz/",
      });
    }

    const candidateSlugs = resolveCandidateSlugs(episodeId);
    const reqCat = category ? String(category).toLowerCase() : null;

    // 1. Collect server embeds from BOTH animesalt.cx and animesalt.ro in parallel
    const [cxRes, roRes] = await Promise.allSettled([
      collectCxServers(candidateSlugs),
      collectRoServers(candidateSlugs),
    ]);

    const cxServers = cxRes.status === "fulfilled" ? cxRes.value : [];
    const roServers = roRes.status === "fulfilled" ? roRes.value : [];
    const allServers = [...cxServers, ...roServers];

    if (allServers.length === 0) {
      return { sources: [], subtitles: [] };
    }

    // Deduplicate embeds across .cx and .ro (e.g. same FireHLS video hash on ravok.buzz & dead as-cdn26.top)
    const seenVideoHashes = new Set();
    const seenNekoIds = new Set();
    const seenDirectUrls = new Set();

    const resolverTasks = [];
    let sawAbyss = false;

    for (const srv of allServers) {
      const urlLower = srv.url.toLowerCase();

      // Skip proprietary in-browser Abyss WebSocket player
      if (
        urlLower.includes("abyssplayer.com") ||
        urlLower.includes("abysscdn.com") ||
        urlLower.includes("multilang.php?data=") ||
        urlLower.includes("/plyr/player.php")
      ) {
        sawAbyss = true;
        continue;
      }

      // FireHLS / VPlayer (/video/<hash>)
      const videoHashMatch = srv.url.match(/\/video\/([a-zA-Z0-9_-]+)/);
      if (videoHashMatch) {
        const hash = videoHashMatch[1];
        if (seenVideoHashes.has(hash)) continue;
        seenVideoHashes.add(hash);

        const labelHint =
          srv.title && !/^server/i.test(srv.title) ? srv.title : "playX";
        resolverTasks.push(
          resolveFireHlsStream(srv.url, srv.originSite, labelHint)
        );
        continue;
      }

      // NekoMux HLS
      const nekoMatch = srv.url.match(/player\.nekomux\.com\/episode\/(\d+)/);
      if (nekoMatch) {
        const nekoId = nekoMatch[1];
        if (seenNekoIds.has(nekoId)) continue;
        seenNekoIds.add(nekoId);

        resolverTasks.push(
          resolveNekomuxStream(srv.url).then((hlsUrl) => {
            if (!hlsUrl) return { sources: [], subtitles: [] };
            return {
              sources: [
                {
                  name: "AnimeSalt NekoMux (Multi-Audio: Hindi / Tamil / Telugu / Eng)",
                  quality: "1080p (Multi-Audio HLS - Hindi Default)",
                  url: hlsUrl,
                  isM3U8: true,
                  headers: {
                    Referer: "https://player.nekomux.com/",
                    Origin: "https://player.nekomux.com",
                  },
                  _providerTag: "nekomux",
                },
              ],
              subtitles: [],
            };
          })
        );
        continue;
      }

      // Direct media (.m3u8 / .mp4 / .mkv / .webm)
      const isDirectMedia =
        urlLower.includes(".m3u8") ||
        Boolean(srv.url.split("?")[0].match(/\.(mp4|mkv|webm)$/i));
      if (isDirectMedia && !seenDirectUrls.has(srv.url)) {
        seenDirectUrls.add(srv.url);
        const isM3u8 = urlLower.includes(".m3u8");
        resolverTasks.push(
          Promise.resolve({
            sources: [
              {
                name: `AnimeSalt (${srv.title || "Direct"})`,
                quality: isM3u8 ? "HLS" : "Direct",
                url: srv.url,
                isM3U8: isM3u8,
                headers: { Referer: `${srv.originSite}/` },
                _providerTag: "direct",
              },
            ],
            subtitles: [],
          })
        );
      }
    }

    // 2. Resolve all non-Abyss embeds in parallel
    const settled = await Promise.allSettled(resolverTasks);
    const candidateSources = [];
    const subtitles = [];
    const seenSubUrls = new Set();

    for (const r of settled) {
      if (r.status !== "fulfilled" || !r.value) continue;
      for (const s of r.value.sources || []) {
        candidateSources.push(s);
      }
      for (const sub of r.value.subtitles || []) {
        if (sub.url && !seenSubUrls.has(sub.url)) {
          seenSubUrls.add(sub.url);
          subtitles.push(sub);
        }
      }
    }

    if (candidateSources.length === 0) {
      if (sawAbyss) {
        console.warn(
          `[AnimeSalt] Only Abyss Player streams found for "${episodeId}". Abyss uses proprietary in-browser WebSocket encryption and cannot be played directly by MPV or mobile video players.`
        );
      }
      return { sources: [], subtitles };
    }

    // 3. Verify reachability of every candidate stream in parallel so we always pick working sources
    const verifications = await Promise.all(
      candidateSources.map((src) => verifyStreamSource(src))
    );

    const workingSources = [];
    const uncertainSources = [];

    candidateSources.forEach((src, i) => {
      const check = verifications[i];
      const streamType = reqCat || "sub";
      const tagged = {
        name: src.name,
        quality: src.quality,
        url: src.url,
        isM3U8: src.isM3U8,
        headers: src.headers,
        lang: streamType,
        type: streamType,
        isDub: reqCat === "dub",
        _providerTag: src._providerTag,
      };

      if (check.status === "verified") {
        tagged._verified = true;
        workingSources.push(tagged);
      } else if (check.status === "uncertain") {
        tagged._verified = false;
        uncertainSources.push(tagged);
      } else {
        console.warn(
          `[AnimeSalt] Dropped broken source "${src.name}" (${check.reason})`
        );
      }
    });

    const finalPool = [...workingSources, ...uncertainSources];

    // Sort: verified sources always come first; for dub prefer Hindi-default NekoMux if working,
    // otherwise prefer FireHLS / playX (full 1080p Multi-Audio + English subtitles) first.
    finalPool.sort((a, b) => {
      if (a._verified !== b._verified) return a._verified ? -1 : 1;
      const tagRank = (tag) => {
        if (reqCat === "dub") {
          if (tag === "nekomux") return 3;
          if (tag === "firehls") return 2;
          return 1;
        }
        if (tag === "firehls") return 3;
        if (tag === "nekomux") return 2;
        return 1;
      };
      return tagRank(b._providerTag) - tagRank(a._providerTag);
    });

    const cleanSources = finalPool.map(
      ({ _providerTag, _verified, ...rest }) => rest
    );

    return {
      sources: cleanSources,
      subtitles,
    };
  } catch (err) {
    console.error("AnimeSalt fetchEpisodeSources error:", err.message);
    return { sources: [], subtitles: [] };
  }
}

/**
 * Fetch latest releases for catalog and home screen from both animesalt.cx and animesalt.ro.
 */
async function fetchRecentEpisodes(page = 1) {
  try {
    const pageNum = normalizePage(page);
    const cxUrl =
      pageNum > 1
        ? `${cxBaseUrl}/series/page/${pageNum}/`
        : `${cxBaseUrl}/series/`;
    const roUrl =
      pageNum > 1
        ? `${roBaseUrl}/anime/page/${pageNum}/?order=update`
        : `${roBaseUrl}/anime/?order=update`;

    const [cxResp, roResp] = await Promise.allSettled([
      httpGet(cxUrl, { headers: { Referer: `${cxBaseUrl}/` }, timeout: 9000 }),
      httpGet(roUrl, { headers: { Referer: `${roBaseUrl}/` }, timeout: 9000 }),
    ]);

    let cxCards = [];
    let cxPag = { currentPage: pageNum, hasNextPage: false, totalPages: pageNum };
    if (
      cxResp.status === "fulfilled" &&
      cxResp.value &&
      cxResp.value.status < 400 &&
      typeof cxResp.value.data === "string"
    ) {
      const $cx = cheerio.load(cxResp.value.data);
      cxCards = parseCxCards($cx);
      cxPag = parsePagination($cx, pageNum);
    }

    let roCards = [];
    let roPag = { currentPage: pageNum, hasNextPage: false, totalPages: pageNum };
    if (
      roResp.status === "fulfilled" &&
      roResp.value &&
      roResp.value.status < 400 &&
      typeof roResp.value.data === "string"
    ) {
      const $ro = cheerio.load(roResp.value.data);
      roCards = parseRoCards($ro);
      roPag = parsePagination($ro, pageNum);
    }

    const results = mergePaginatedCards(cxCards, roCards, pageNum, recentSeenCache);
    const totalPages = Math.max(
      cxCards.length > 0 ? cxPag.totalPages : 0,
      roCards.length > 0 ? roPag.totalPages : 0,
      results.length > 0 ? pageNum : 0
    );
    const hasNextPage =
      (cxCards.length > 0 && cxPag.hasNextPage) ||
      (roCards.length > 0 && roPag.hasNextPage);

    return {
      currentPage: pageNum,
      current_page: pageNum,
      page: pageNum,
      hasNextPage,
      totalPages,
      results,
    };
  } catch (err) {
    console.error("AnimeSalt fetchRecentEpisodes error:", err.message);
    const pNum = normalizePage(page);
    return {
      currentPage: pNum,
      current_page: pNum,
      page: pNum,
      hasNextPage: false,
      totalPages: 0,
      results: [],
    };
  }
}

async function healId({ oldId, malid, title, folderName }) {
  try {
    const q = title || folderName || oldId;
    if (!q) return null;
    const searchRes = await SearchAnime(q, 1);
    if (searchRes && searchRes.results && searchRes.results.length > 0) {
      return searchRes.results[0].id;
    }
  } catch (_) {}
  return null;
}

module.exports = {
  name: "animesalt",
  version: "1.0.9",
  baseUrl,
  headerRules,
  subrequestHeaders,
  bypassDomains,
  healId,
  SearchAnime,
  SearchMovie: SearchAnime,
  AnimeInfo,
  MovieInfo: AnimeInfo,
  fetchEpisode,
  fetchEpisodeSources,
  fetchRecentEpisodes,
  fetchTrending: fetchRecentEpisodes,
};
