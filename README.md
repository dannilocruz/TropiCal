# TropiCal
A data processing tool for the Thermo Scientific™ Niton™ XL5 Plus handheld analyzer.
Not affiliated with or endorsed by Thermo Fisher Scientific.

## How to use
- Website: https://tropical.danilocruz.earth/
- Containers (Docker Hub): [dannilocruz/tropical](https://hub.docker.com/repository/docker/dannilocruz/tropical/general)

## What TropiCal does
- Built-in calibration exports: uses the XL5 analyzer’s built-in calibration to export major elements as oxides (wt%) and trace elements in ppm — ready for geochemical interpretation.
- Empirical quantification via `.quant` files: when supplied a `.quant` produced by [CloudCal-XL5-fork](https://github.com/dannilocruz/CloudCal-XL5-fork), TropiCal quantifies raw spectra and exports results in the same format (majors as oxides wt%, traces in ppm).
- Raw spectra exports: produces XL5 raw spectra with metadata suitable for custom/empirical calibration. These files are compatible with CloudCal-XL5-fork.

> **Important:** If no `.quant` is provided, TropiCal falls back to a default calibration based on the analyzer in UF's Department of Geological Sciences. **Do not** use this default unless your data were collected on this instrument.

## Compatibility
- Works with calibration `.quant` files produced by CloudCal-XL5-fork.
- Exports XL5 raw spectra and metadata in a format CloudCal-XL5-fork can open for empirical calibration.

## Related repositories
- CloudCal-XL5-fork: https://github.com/dannilocruz/CloudCal-XL5-fork

## License
- TropiCal includes components derived from [CloudCal](https://github.com/leedrake5/CloudCal) (GPL-3.0).  
- As distributed here, TropiCal is licensed under GPL-3.0. See `LICENSE` for details.  

## Citation
If this tool supports your work, please cite both:
- TropiCal: Cruz, D. J. N. (2025). TropiCal (v1.0). Zenodo. https://doi.org/10.5281/zenodo.17137037
- CloudCal: Lee Drake. (2019). leedrake5/CloudCal: Neural Networks (v3.0). Zenodo. https://doi.org/10.5281/zenodo.2596154

## Disclaimer

- TropiCal is provided “as is” without warranty of any kind, express or implied. It is intended for research and educational use.
- TropiCal is not affiliated with or endorsed by Thermo Fisher Scientific. *Thermo Scientific™* and *Niton™* are trademarks of Thermo Fisher Scientific and its affiliates, used here for identification only.
- TropiCal is not affiliated with the original CloudCal maintainers.

