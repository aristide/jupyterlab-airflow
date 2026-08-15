# Bundled font licenses

The `.woff2` files in this directory are third-party typefaces redistributed
with this extension. Each is the **latin subset only**, generated from the
Google Fonts CSS API. None were modified beyond subsetting.

They are vendored rather than fetched at runtime so the extension works in
offline and air-gapped deployments — see the header of `style/fonts.css`.

| Family         | Files                            | License                                                           | Copyright                                                                                      |
| -------------- | -------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Montserrat     | `montserrat-{500,600,700}.woff2` | [SIL Open Font License 1.1](https://openfontlicense.org/)         | Copyright 2011 The Montserrat Project Authors (https://github.com/JulietaUla/Montserrat)       |
| Roboto         | `roboto-{300,400,500,700}.woff2` | [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) | Copyright 2011 Google Inc.                                                                     |
| JetBrains Mono | `jetbrains-mono-{400,500}.woff2` | [SIL Open Font License 1.1](https://openfontlicense.org/)         | Copyright 2020 The JetBrains Mono Project Authors (https://github.com/JetBrains/JetBrainsMono) |

Both licenses permit redistribution, including bundled inside a larger work.
The OFL requires that the fonts not be sold on their own and that any _modified_
version be renamed — neither applies here, as these are unmodified subsets
shipped as part of the extension.

`MANIFEST.txt` records the family/weight/filename mapping the fetch produced.
