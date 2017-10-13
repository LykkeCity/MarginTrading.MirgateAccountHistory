using System.Threading.Tasks;
using Lykke.SettingsReader;

namespace MarginTrading.MirgateAccountHistory.Helpers
{
    public static class StringExtensions
    {
        /// <summary>
        /// Makes a <see cref="IReloadingManager{T}"/> from the string
        /// </summary>
        public static IReloadingManager<T> MakeSettings<T>(this T str)
        {
            return new StaticSettingsManager<T>(str);
        }

        private class StaticSettingsManager<T> : IReloadingManager<T>
        {
            public StaticSettingsManager(T currentValue)
            {
                HasLoaded = true;
                CurrentValue = currentValue;
            }

            public Task<T> Reload()
            {
                return Task.FromResult(CurrentValue);
            }

            public bool HasLoaded { get; }
            public T CurrentValue { get; }
        }
    }
}
