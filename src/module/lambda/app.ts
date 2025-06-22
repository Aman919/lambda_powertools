import { z } from 'zod';
import * as jmespath from 'jmespath';
import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';

// In-memory store for idempotency
const processedRequests = new Set<string>();

// Zod schema for input validation
const StudentSchema = z.object({
  name: z.string(),
  Subject: z.object({
    science: z.number().min(0).max(100),
    maths: z.number().min(0).max(100),
    result: z.enum(['pass', 'fail']),
  }),
  Attendance: z.number().min(0).max(100),
});

const InputSchema = z.object({
  result: z.array(StudentSchema),
});


export const handler = async (event: APIGatewayProxyEvent, context: Context): Promise<APIGatewayProxyResult> => {
  try {
    console.log('INFO: Received event:', JSON.stringify(event, null, 2));

    // Parse and validate input
    let body: unknown;
    try {
      body = JSON.parse(event.body || '{}');
    } catch (err) {
      console.error('ERROR: Invalid JSON input');
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Invalid JSON input' }),
      };
    }

    const validatedData = InputSchema.safeParse(body);
    if (!validatedData.success) {
      console.error('ERROR: Validation failed:', validatedData.error);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Invalid input', details: validatedData.error }),
      };
    }

    const students = validatedData.data.result;

    // Idempotency check
    const idempotencyKey: string = event.requestContext?.requestId || context.awsRequestId;
    if (processedRequests.has(idempotencyKey)) {
      console.log('INFO: Idempotent request detected:', idempotencyKey);
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Request already processed' }),
      };
    }
    processedRequests.add(idempotencyKey);

    // operation using  JMESPath
    const results = {
      studentNames: jmespath.search(students, '[*].name'),
      scienceMarks: jmespath.search(students, '[*].Subject.science'),
      scienceAbove80: jmespath.search(students, '[?Subject.science > `80`].name'),
      passedStudents: jmespath.search(students, '[?Subject.result == `pass`].name'),
      passedLowAttendance: jmespath.search(students, '[?Subject.result == `pass` && Attendance < `50`].name'),
      perfectScore: jmespath.search(students, '[?Subject.science == `100` || Subject.maths == `100`].name'),
      nameAndResult: jmespath.search(students, '[*].{Name: name, Result: Subject.result}'),
    };

    console.log('INFO: Operations completed successfully');
    return {
      statusCode: 200,
      body: JSON.stringify(results),
    };
  } catch (err) {
    console.error('ERROR: Unexpected error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal server error' }),
    };
  }
};
