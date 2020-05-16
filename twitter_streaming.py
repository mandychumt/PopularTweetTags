import tweepy
import random
import sys

class MyStreamListener(tweepy.StreamListener):
    def __init__(self, output_file, sample = [], tag_count = {}, seq_num = 0):
        super(MyStreamListener, self).__init__()
        self.output_file = output_file
        self.sample = sample
        self.tag_count = tag_count
        self.seq_num = seq_num
    
    def add_tag(self, tags_ls):
        for tag in tags_ls:
            self.tag_count[tag] = self.tag_count.get(tag, 0) + 1
    
    def remove_tag(self, tags_ls):
        for tag in tags_ls:
            self.tag_count[tag] -= 1
    
    def on_status(self, status):
        tags = status.entities['hashtags']
        if tags:
            self.seq_num += 1
            tags_ls = [d['text'] for d in tags]
            # print(tags_ls)
            if self.seq_num <= 100:
                self.sample.append(tags_ls)
                self.add_tag(tags_ls)
            else:
                randint = random.randint(1, self.seq_num)
                if randint <= 100:
                    # print('replace')
                    replace_sample_index = random.randint(0, 99)
                    self.remove_tag(self.sample[replace_sample_index])
                    self.sample[replace_sample_index] = tags_ls
                    self.add_tag(tags_ls)
            # print('The number of tweets with tags from the beginning:', self.seq_num)
            # print(self.tag_count)
            # print(len(self.sample), '\n')
            self.output()
    
    def output(self):
        tag_count_ls = list(self.tag_count.items())
        tag_count_ls.sort(key=lambda x: (-x[1],x[0]))
        if self.seq_num == 1:
            fh = open(self.output_file, 'w')
        else:
            fh = open(self.output_file, 'a')
        fh.write('The number of tweets with tags from the beginning: ' + str(self.seq_num) + '\n')
        for t in tag_count_ls:
            if t[1] > 0:
                fh.write(t[0] + ' : ' + str(t[1]) + '\n')
        fh.write('\n')
        fh.close()

def main():

	port = sys.argv[1]
	output_file = sys.argv[2]

	auth = tweepy.OAuthHandler('d2bjf7WQkPJ0SQlSEJtQvnySZ', 'OFSfgy7n8EoLjkTFtAW4p6rwqgvmvCf3OCM8oMHdur0mCnHEyK')
	auth.set_access_token('1251181262410153990-y2eroRyGZfKSu7xDAcVoxb08lhQRLQ', 'YiaPM4mo8wjemNfUvxoHq5lboO6BGyCeNNV5kzz1yzRZy')
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

	myStreamListener = MyStreamListener(output_file)
	myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
	myStream.filter(languages=['en'], track=['#'])

if __name__ == "__main__":
	while True:
		try:
			main()
			break
		except:
			print('********* Exception + 1 *********')
			continue
