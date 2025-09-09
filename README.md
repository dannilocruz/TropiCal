# TropiCal
*A data-reduction tool for the Thermo Scientific Niton XL5 Plus analyzer*

## How to use
- **Website:** https://tropical.danilocruz.earth/
- **Containers (Docker Hub):** `dannilocruz/tropical`

> Instructions on how to run locally may be added later.

## What TropiCal does
- Built-in calibration exports: uses the XL5 analyzer’s built-in calibration to export major elements as oxides (wt%) and trace elements in ppm — ready for geochemical interpretation.
- Empirical quantification via `.quant` files: when supplied a `.quant` produced by [CloudCal-XL5-fork](https://github.com/dannilocruz/CloudCal-XL5-fork), TropiCal quantifies raw spectra and exports results in the same format (majors as oxides wt%, traces in ppm).
- Raw spectra exports: produces XL5 raw spectra with metadata suitable for custom/empirical calibration. These files are compatible with CloudCal-XL5-fork.

> **Important:** If no `.quant` is provided, TropiCal falls back to a default calibration based on the analyzer in UF's Department of Geological Sciences. **Do not** use this default unless your data were collected on this instrument.

## Compatibility
- Works with calibration `.quant` files produced by CloudCal-XL5-fork.
- Exports XL5 raw spectra and metadata in a format CloudCal-XL5-fork can open for empirical calibration.

## Related repositories
- CloudCal (upstream): https://github.com/leedrake5/CloudCal
- CloudCal-XL5-fork (fork used to create `.quant` calibrations): https://github.com/dannilocruz/CloudCal-XL5-fork

## License
TropiCal includes components derived from CloudCal (GPL-3.0).  
As distributed here, TropiCal is licensed under GPL-3.0. See `LICENSE` for details.  
TropiCal is not affiliated with the original CloudCal maintainers.

## Citation
If this tool supports your work, please cite both:
- TropiCal: TropiCal v.1.0. GitHub: https://github.com/dannilocruz/TropiCal
- CloudCal: Drake, B.L. 2018. *CloudCal v3.0*. doi: 10.5281/zenodo.2596154

## Acknowledgments
- The CloudCal creators and contributors for the original project.
- Colleagues and users contributing XL5 datasets and feedback.
