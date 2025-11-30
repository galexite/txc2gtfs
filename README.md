# txc2gtfs

**txc2gtfs** is a library for converting UK public transport data from [TransXChange](https://www.gov.uk/government/collections/transxchange) format
into [GTFS](https://developers.google.com/transit/gtfs) that can be used with various routing engines such as OpenTripPlanner.

## Features

 - Reads TransXchange xml-files and converts into GTFS feed with all necessary information
 according the General Transit Feed Specification.
 - Works and tested against different TransXchange schemas (TfL schema and TXC 2.1)
 - Combines multiple TransXchange files into a single GTFS feed if present in the same folder.
 - Finds and reads all XML files present in ZipFiles, nested ZipFiles and unpacked directories.
 - Uses multiprocessing to parallelize the conversion process.
 - Parses bank holidays (from [gov.uk](https://www.gov.uk/bank-holidays)) affecting transit operations at the given time span of the TransXChange feed, which are written to calendar_dates.txt.
 - Reads and updates stop information automatically from NaPTAN website.

## Why yet another converter?

There are numerous TransXChange to GTFS converters written in different programming languages.
However, after testing many of them, it was hard to find a tool that would:

 1. work in general (without ad-hoc modifications)
 2. parse all important information from the TransXChange according GTFS specification.
 3. work with different TransXChange schema versions
 4. be well maintained
 5. be easy to use in all operating systems
 6. include appropriate tests (crucial for maintenance).

Hence, this Python package was written which aims at meeting the aforementioned requirements.
It's not the fastest library out there (written in Python) but multiprocessing gives a bit of boost
if having a decent computer with multiple cores.

### Requirements

txc2gtfs has following dependencies:

 - pandas

## Basic usage

Once checked out, you can install this package using a Python package manager, for example, pip:

```sh
pip install -e .
```

Then, either use the command line interface:

```sh
txc2gtfs path/to/transxchange_data.xml -o path/to/my_converted_gtfs.zip
```

Or use it as a Python library:

```python
>>> import txc2gtfs
>>> data_dir_for_transxchange_files = "data/my_transxchange_files"
>>> output_path = "data/my_converted_gtfs.zip"
>>> txc2gtfs.convert(data_dir_for_transxchange_files, output_path)
```

See the docstring on `convert` for more information.

## Output

After you have successfully converted the TransXchange into GTFS, you can start doing
multimodal routing with your favourite routing engine such as OpenTripPlanner:

![OTP_example_in_London](img/London_multimodal_route.PNG)

## Citation

If you use this tool for research purposes, we encourage you to cite this work:

 - Henrikki Tenkanen. (2020). txc2gtfs (Version v0.4.0). Zenodo. http://doi.org/10.5281/zenodo.3631972

## Developers

- Henrikki Tenkanen, University College London
