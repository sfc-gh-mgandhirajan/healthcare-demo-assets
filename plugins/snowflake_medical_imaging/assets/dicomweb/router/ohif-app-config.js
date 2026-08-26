/** @type {AppTypes.Config} */

/*
 * OHIF runtime configuration for Voxel.
 *
 * ===========================================================================
 * SAME-ORIGIN, DELIBERATELY
 *
 * The DICOMweb endpoints are RELATIVE paths, not an absolute URL, because the
 * router and the gateway sit behind ONE public SPCS endpoint. Two public
 * endpoints would mean two hostnames, and the ingress proxy's baseline CSP is
 * `connect-src 'self'` -- every fetch from the SPA to a different hostname is
 * hard-blocked, and CORS cannot loosen it because connect-src is evaluated in
 * the sending document.
 *
 * So relative paths are not a style choice. An absolute URL here is a broken
 * viewer.
 *
 * ===========================================================================
 * DO NOT ADD AN `oidc` KEY
 *
 * Snowflake terminates authentication at the ingress proxy, so the browser is
 * already an authenticated Snowflake session before this file is even fetched.
 * OHIF must not run its own OIDC flow on top of that.
 *
 * The obvious way to express "no OIDC" is `oidc: []`. THAT PRODUCES A BLANK
 * PAGE. OHIF checks whether the key is PRESENT and then dereferences
 * `config.oidc[0].authority` without checking the array is non-empty:
 *
 *     Uncaught TypeError: Cannot read properties of undefined (reading 'authority')
 *
 * Present-but-empty is the worst case -- it passes the existence check and then
 * throws. React never mounts, #root stays empty, and there is no visible error.
 *
 * The correct encoding of "no OIDC" is to OMIT THE KEY ENTIRELY.
 * ===========================================================================
 */
window.config = {
  name: 'config/voxel.js',

  /* null, matching OHIF's own default. The app is served from the domain root by
   * nginx, and '/' is not the same as null to OHIF's router. */
  routerBasename: null,

  /* Populated by the bundle at boot -- these are empty in OHIF's own shipped
   * default config too, so do not "fix" them by hand. Console confirms modes
   * register: "Registering worklist route / /". */
  extensions: [],
  modes: [],
  customizationService: {},

  showStudyList: true,

  /* Some Windows systems fail above 3. OHIF's own default. */
  maxNumberOfWebWorkers: 3,
  showCPUFallbackMessage: true,
  showLoadingIndicator: true,
  strictZSpacingForVolumeViewport: true,
  showErrorDetails: 'always',

  maxNumRequests: {
    interaction: 100,
    thumbnail: 5,
    prefetch: 25,
  },

  defaultDataSourceName: 'voxel',

  dataSources: [
    {
      namespace: '@ohif/extension-default.dataSourcesModule.dicomweb',
      sourceName: 'voxel',
      configuration: {
        friendlyName: 'Voxel (Snowflake)',
        name: 'voxel',

        wadoUriRoot: '/dicom-web',
        qidoRoot: '/dicom-web',
        wadoRoot: '/dicom-web',

        /* The gateway rejects unknown QIDO attributes with 400 rather than
         * silently ignoring them, so advertising unsupported capabilities here
         * turns into hard request failures. These flags match
         * CONFORMANCE_EXPECTATION exactly. */
        qidoSupportsIncludeField: false,
        supportsFuzzyMatching: false,
        supportsWildcard: true,
        supportsReject: false,

        /* Voxel serves frames in their ORIGINAL transfer syntax as byte-range
         * passthrough -- no server-side decode, no transcode. The browser's WASM
         * codecs do the work, which is why the gateway image ships no JPEG
         * libraries at all. */
        imageRendering: 'wadors',
        thumbnailRendering: 'wadors',
        enableStudyLazyLoad: true,

        /* One request per scroll window instead of one per frame. A browser caps
         * roughly 6 concurrent connections per origin, so 500 individual frame
         * GETs serialize into ~83 rounds of head-of-line blocking no matter how
         * fast the server is. The gateway accepts a comma-separated frame list
         * for exactly this. */
        singlepart: false,

        /* The gateway does not emit bulkdata URI references.
         * CONFORMANCE_EXPECTATION declares this NOT_IMPLEMENTED. */
        bulkDataURI: { enabled: false },

        /* Cookies carry the Snowflake ingress session. Without this every
         * DICOMweb request from the SPA arrives unauthenticated and the proxy
         * redirects to a sign-in page, which inside an XHR looks like an opaque
         * failure rather than an auth problem. */
        requestOptions: {
          withCredentials: true,
        },
      },
    },
  ],
};
