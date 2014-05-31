This is an implementation of "Hokusai - Sketching Streams in Real Time" by Sergiy Matusevych, Alex Smola, Amr Ahmed

It maintains a time-series of count-min sketches, using aggregations to reduce
the space (and accuracy) used by older sketches.

This paper is available on-line: http://www.auai.org/uai2012/papers/231.pdf

Alex Smola has given a number of lectures covering the ideas from the paper.
Here is one recording: https://www.youtube.com/watch?v=FQoJV88ve0g

The 'sketch' library has all the algorithms from the paper.  This is more or
less complete.  I've tested it (by 'inspection') during development.  Proper
tests and test data are at the top of my list of things to do.

The 'hokud' package is a simple server that exposes the sketch API.  This is
where development will focus, so there's an actual deployable solution instead
of a package full of math.

This package relies heavily on https://github.com/dustin/go-probably for the
count-min sketch implementation.  The algorithms from the paper which depended
on the internals of the count-min sketch implementation were added to
go-probably instead of here.
