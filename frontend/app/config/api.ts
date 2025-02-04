export const API_CONFIG = {
  apiBaseUrl: process.env.LIBRESSION_API_BASE_URL || "http://localhost:8000",
  // Server side and caller side io base urls in theory should be same
  // but can be different e.g. deployed in docker, app calling from local machine
  // needs different ioBaseUrl when app directly calls ioBaseUrl
  ioBaseUrlServer:
    process.env.LIBRESSION_IO_BASE_URL_SERVER || "https://webdav",
  ioBaseUrlCaller:
    process.env.LIBRESSION_IO_BASE_URL_CALLER || "https://localhost:8443",
  endpoints: {
    upload: "/libression/v1/upload",
    copy: "/libression/v1/copy",
    delete: "/libression/v1/delete",
    showDirContents: "/libression/v1/show_dir_contents",
    filesInfo: "/libression/v1/files_info",
    filesUrls: "/libression/v1/files_urls",
    thumbnailsUrls: "/libression/v1/thumbnails_urls",
    updateTags: "/libression/v1/update_tags",
    searchByTags: "/libression/v1/search_by_tags",
  },
} as const;

export const getApiUrl = (
  endpoint: keyof typeof API_CONFIG.endpoints,
): string => {
  return `${API_CONFIG.apiBaseUrl}${API_CONFIG.endpoints[endpoint]}`;
};

export const transformWebDAVUrl = (url: string): string => {
  // Remove any port number from the URL first
  const urlWithoutPort = url.replace(/:\d+(?=\/)/, "");
  // Replace the server URL with caller URL
  return urlWithoutPort.replace(
    API_CONFIG.ioBaseUrlServer,
    API_CONFIG.ioBaseUrlCaller,
  );
};
