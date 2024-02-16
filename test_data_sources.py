import vnstock
import pandas as pd


def main():
    print(vnstock.company_profile('TCB').T)


if __name__ == '__main__':
    main()
