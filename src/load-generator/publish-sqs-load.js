const crypto = require('crypto');
const { SendMessageBatchCommand, SQSClient } = require('@aws-sdk/client-sqs');

const queueUrl = process.env.SQS_ORDER_EVENTS_QUEUE_URL;
const region = process.env.AWS_REGION || 'us-east-1';
const totalMessages = Number(process.env.TOTAL_MESSAGES || 180);
const batchSize = Math.min(Math.max(Number(process.env.BATCH_SIZE || 10), 1), 10);

if (!queueUrl) {
    console.error('[ERROR] SQS_ORDER_EVENTS_QUEUE_URL is required');
    process.exit(1);
}

const sqs = new SQSClient({ region });

const chunk = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) {
        out.push(arr.slice(i, i + size));
    }
    return out;
};

const buildMessage = () => {
    const quantity = Math.ceil(Math.random() * 3);
    const amount = quantity * 10;
    return {
        type: 'order.created',
        data: {
            orderId: crypto.randomUUID(),
            ticketId: crypto.randomUUID(),
            quantity,
            status: 'created',
            amount,
            createdAt: new Date().toISOString()
        },
        emittedAt: new Date().toISOString()
    };
};

const main = async () => {
    const payloads = Array.from({ length: totalMessages }).map(() => buildMessage());
    const batches = chunk(payloads, batchSize);

    let sent = 0;
    for (const batch of batches) {
        const Entries = batch.map((payload, index) => ({
            Id: `${Date.now()}-${index}-${Math.random().toString(36).slice(2, 8)}`,
            MessageBody: JSON.stringify(payload)
        }));

        const result = await sqs.send(
            new SendMessageBatchCommand({
                QueueUrl: queueUrl,
                Entries
            })
        );

        const failed = (result.Failed || []).length;
        sent += Entries.length - failed;
    }

    console.log(`[DONE] Published ${sent}/${totalMessages} messages to ${queueUrl}`);
};

main().catch((err) => {
    console.error('[ERROR] Failed to publish SQS load', err);
    process.exit(1);
});
