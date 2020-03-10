#include "Vertica.h"
#include <math.h>
#include <float.h>
#include <vector>
#include <algorithm>
#include <limits>

using namespace Vertica;
using namespace std;

class Mad : public TransformFunction
{
	vfloat *iarray ;		// Pointer to the array containing the input values
	vfloat *sarray ;		// Pointer to the array containing the sorted values used to calculate MAD
	size_t setsize  ;		// Size of the array

	virtual void setup(ServerInterface &srvInterface,
					   const SizedColumnTypes &argTypes)
	{
        setsize = 10 ;		// Setting default array length (used if no parameter)
        iarray = 0 ;		// Array with input values (sorted by event timestamp)
        sarray = 0 ;		// Array with input values (sorted by value to calculate median)

		// Parameter (setsize) evaluation
		ParamReader params = srvInterface.getParamReader();	// Get ParameterReader
		if (params.containsParameter("setsize")) {			// Check if "setsize" param was used
		    setsize = (size_t)params.getIntRef("setsize");	// Get "setsize" parameter value 
			if ( setsize < 1 )  							// Avoid zero/negative setsize values
				vt_report_error ( 100 , "Invalid setsize" ) ;
		}

		// Allocate memory for iarray
		if ( ( iarray = (vfloat *)malloc((size_t)setsize * sizeof(vfloat)) ) == NULL ) 
			vt_report_error ( 110 , "Error allocating iarray memory" ) ;

		// Allocate memory for sarray
		if ( ( sarray = (vfloat *)malloc((size_t)setsize * sizeof(vfloat)) ) == NULL ) 
			vt_report_error ( 110 , "Error allocating sarray memory" ) ;
	}

	virtual void destroy(ServerInterface &srvInterface,
						 const SizedColumnTypes &argTypes)
	{
		// Free memory allocated during setup
		if ( iarray )
			free( iarray ) ;
		if ( sarray )
			free( sarray ) ;
	}
	virtual void processPartition(ServerInterface & srvInterface,
                                  PartitionReader & inputReader,
								  PartitionWriter & outputWriter)
   	{
		try
		{
			const SizedColumnTypes & inTypes = inputReader.getTypeMetaData();
			std::vector<size_t> argCols;		// Arg column indexes
			inTypes.getArgumentColumns(argCols);
			size_t valIdx = argCols.at(0);
			size_t i = 0 ;						// Array index
			vfloat median = 0;					// Median for the current set of input values
			vfloat mad = 0;						// Median Absolute Deviation for the current set of input values
			vfloat value = 0 ;					// Next input value
			vfloat cconst = 1.4826 ;			// Default Consistency Constant (for Normal Distribution)
			vint rn = 1 ;						// Row Number

			// Parameter (Consistency Constant) evaluation
			ParamReader params = srvInterface.getParamReader();	// Get ParameterReader
			if (params.containsParameter("cconst")) 			// Check if "setsize" param was used
				cconst = params.getFloatRef("cconst");

			// Read input values & calculate MAD
			do
			{
				value = inputReader.getFloatRef(valIdx) ;	// Read next input value
				if ( vfloatIsNull(value)) {					// If the value is NULL...
					outputWriter.setInt(0, rn++);			// Row Number
					outputWriter.setNull(1) ;				// Return NULL & skip it
					outputWriter.setNull(2) ;				// Return NULL & skip it
					outputWriter.setNull(3) ;				// Return NULL & skip it
				} else if ( i + 1 < setsize ) {				// Not enough elements yet...
					outputWriter.setInt(0, rn++);			// Row Number
					outputWriter.setNull(1) ;				// Return NULL
					outputWriter.setNull(2) ;				// Return NULL
					outputWriter.setNull(3) ;				// Return NULL
				 	iarray[i++] = value ;					// Set next input array element
				} else {
					// Add the new element at the end...
				 	iarray[setsize-1] = value ;				// Set next input array element
					// Dump iarray into sarray..a (and, yes, we can use std::copy()).
					memcpy(sarray, iarray, setsize * sizeof(vfloat)) ;
					// Sort sarray...
					std::sort(sarray, sarray+setsize);
					// Calculate Median...
					if ( setsize % 2 ) 	// odd number of array elements
						median = sarray[setsize/2] ;
					else
						median = ( sarray[setsize/2] + sarray[setsize/2 - 1] ) / 2 ;
					// Re-calculate sarray elements as abs(value-median)...
					for ( i=0 ; i < setsize ; i++ ) 
						sarray[i] = abs ( sarray[i] - median ) ;
					// Re-sort sarray...
					std::sort(sarray, sarray+setsize);
					// Get Median Absolute Deviation...
					if ( setsize % 2 ) 	// odd number of array elements
						mad = cconst * sarray[setsize/2] ;
					else
						mad = cconst * ( sarray[setsize/2] + sarray[setsize/2 - 1] ) / 2 ;
					// Output value/mad deviation
					outputWriter.setInt( 0 , rn++);				// Row Number
					outputWriter.setFloat ( 1 , median );		// Median
					outputWriter.setFloat ( 2 , mad );			// Mad
					outputWriter.setFloat ( 3 , abs ( value - median ) / mad );	// Cutoff
					// Left-shift the array to remove the "older" element...
					memmove(iarray, iarray+1, sizeof(vfloat)*(setsize - 1));
				}
				outputWriter.next() ;				// Advance outputwriter "cursor"
			} while ( inputReader.next() ) ;		// Advance inputreader "cursor"
		}
		catch (exception& e)
		{
			vt_report_error(0, "Exception while processing partition: [%s]", e.what());
		}
	}
};

class MadFactory : public TransformFunctionFactory
{
	virtual void getPrototype(ServerInterface &srvInterface,
							  ColumnTypes &argTypes,
							  ColumnTypes &returnType)
	{
		argTypes.addFloat();
		returnType.addInt();
		returnType.addFloat();
		returnType.addFloat();
		returnType.addFloat();
	}
	virtual void getReturnType(ServerInterface &srvInterface,
							   const SizedColumnTypes &inputTypes,
							   SizedColumnTypes &outputTypes)
	{
		outputTypes.addInt("rownum");
		outputTypes.addFloat("median");
		outputTypes.addFloat("mad");
		outputTypes.addFloat("cutoff");
	}
	virtual void getParameterType(ServerInterface &srvInterface,
								  SizedColumnTypes &parameterTypes)
	{
		parameterTypes.addInt("setsize");
		parameterTypes.addFloat("cconst");
	}
	virtual TransformFunction *createTransformFunction( ServerInterface &srvInterface )
	{
		return vt_createFuncObject<Mad>(srvInterface.allocator);
	}
};

RegisterFactory(MadFactory);
