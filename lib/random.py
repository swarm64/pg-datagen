
import hashlib
import random
import string

from datetime import timedelta
from uuid import uuid4

import mimesis.random as mimesis_random
import numpy as np

from mimesis.schema import Field
from mimesis.enums import Algorithm
from numpy.random import default_rng

from .random_data import RandomData

STR_SEQ = string.ascii_letters + string.digits

class Random:
    UTF8_ALPHABET = [
        chr(code) for this_range in [
            (0x0021, 0x0021),
            (0x0023, 0x0026),
            (0x0028, 0x007E),
            (0x00A1, 0x00AC),
            (0x00AE, 0x00FF),
            (0x0100, 0x017F),
            (0x0180, 0x024F),
            (0x2C60, 0x2C7F),
            (0x16A0, 0x16F0),
            (0x0370, 0x0377),
            (0x037A, 0x037E),
            (0x0384, 0x038A),
            (0x038C, 0x038C),
        ] for code in range(this_range[0], this_range[1] + 1)
    ]

    def __init__(self, seed):
        self.seed = seed
        self.md5_counter = 0
        self.rng = default_rng(seed=seed)
        self.field = Field('en', seed=seed)

        random.seed(seed)

    def sample_income(self, mean_income, median_income, samples=1):
        """Get samples of income.

        Uses a log-normal distribution which is centric
        around mean/median income. Rounds to thousands. For formulas see
        http://www.statlit.org/pdf/2018-Schield-ASA.pdf.
        """
        mu = np.log(median_income)
        sigma = np.sqrt(2.0 * np.log(mean_income / median_income))
        values = self.rng.lognormal(sigma=sigma, mean=mu, size=samples)
        return [int(value / 1000) * 1000 for value in values]

    @classmethod
    def _int_to_granularity(cls, value, granularity):
        return int(int(value / granularity) * granularity)

    def whole_number(self, start, end, granularity=1):
        """Produce a random whole number with the given granularity."""
        number = self.field('integer_number', start=start, end=end)
        return Random._int_to_granularity(number, granularity)

    def whole_number_lognormal(self, mean, median, granularity=1, upper_limit=None):
        """Produce a random log-normal distributed number."""
        number = self.sample_income(mean, median)[0]
        number = Random._int_to_granularity(number, granularity)

        return min(number, upper_limit) if upper_limit else number

    def bool_sample(self, probability_true, size=1):
        """Produce true/false with a given probability of true."""
        if size == 1:
            return self.rng.binomial(1, probability_true, size=size)[0] == 1

        return self.rng.binomial(1, probability_true, size=size) == 1

    def fraction(self, start, end, granularity):
        """Get a fraction within bounds."""
        number = self.whole_number(start * granularity, end * granularity,
                                   granularity=granularity)
        return number / granularity

    def employment(self):
        """Employment categories with a fixed probability."""
        sample = self.rng.random()
        if sample <= 0.05:
            return 'UNEMPLOYED'

        if sample <= 0.15:
            return 'SELF EMPLOYED'

        return 'EMPLOYED'

    def num_children(self):
        """Number of children with fixed probabilities."""
        sample = self.rng.random()
        if sample <= 0.1:
            return 0

        if sample <= 0.65:
            return 1

        if sample <= 0.85:
            return 2

        return 3

    def uuid(self):
        """Returns a UUID4"""
        return uuid4()

    def words(self, num_words):
        """Returns a space-joined string of random words."""
        return ' '.join(self.field('words', quantity=num_words))

    def interest_rate(self, requested_interest):
        """Get a banks interest rate for some loan.

        Based on the requested interest with a margin added.
        """
        interest_margin = round(min(self.rng.lognormal(mean=0.05), 2.0), 2)
        return requested_interest + interest_margin

    def uniform(self, start, end, precision=2):
        """Get a uniform distributed random float number within limits."""
        return round(self.rng.uniform(start, end), precision)

    def random_text(self, min_words, max_words):
        """Get random text with min/max amount of words."""
        return self.words(self.whole_number(min_words, max_words, 1))

    def add_processing_time(self, base, mean_time):
        """Return processing time a bank has. Units are minutes."""
        delta = timedelta(minutes=self.rng.exponential(scale=mean_time))
        return base + delta

    def unicode(self, length):
        """Returns randomized UTF-8 characters of any length."""
        return ''.join(self.rng.choice(Random.UTF8_ALPHABET) for _ in range(length))

    def mimesis(self, what):
        """Returns X from mimesis."""
        return self.field(what)

    def md5(self, prefix):
        """Returns random md5 string."""
        data = f'{ prefix } { self.seed } { self.md5_counter }'.encode('utf-8')
        data_md5 = hashlib.new('md5', data, usedforsecurity=False)
        self.md5_counter += 1

        return data_md5.hexdigest()

    def choose_from_list(self, choices, picks=None, probs=None):
        """Returns a choice from a provided list."""
        values = self.rng.choice(choices, size=picks, p=probs)
        return values

    def data(self, uuid, data_type, serialization_type, length):
        """Get random data."""
        return RandomData(self, uuid, data_type, serialization_type, length)

    def string(self, length):
        """Generate a random string of length between min & max chars."""
        return mimesis_random.random.generate_string(
            str_seq=STR_SEQ, length=length)

    def varchar(self, max_chars=255):
        length = self.whole_number(1, max_chars)
        return self.string(length)

    def text(self):
        return self.random_text(5, 100)

    def int2(self):
        return self.whole_number(-32768, 32767)

    def int4(self):
        return self.whole_number(-2147483648, 2147483646)

    def int8(self):
        return self.whole_number(-9223372036854775808, 9223372036854775806)

    def timestamp(self):
        return self.mimesis('datetime')

    def timestamptz(self):
        return self.timestamp()

    def date(self):
        return self.mimesis('date')

    def numeric(self, precision, scale):
        # Note: do precision correctly
        return self.fraction(-1000, 1000, scale)

    def bpchar(self, length):
        return self.string(length)
