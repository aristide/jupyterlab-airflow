// Allow importing raw SVG markup as a string (see webpack svg asset loader
// configured by @jupyterlab/builder).
declare module '*.svg' {
  const value: string;
  export default value;
}

// Allow side-effect CSS imports (e.g. ReactFlow base styles) from TypeScript.
declare module '*.css';
